import os
import re
import copy
import secrets
import cloudpickle
from inspect import signature
import clusterlib.file as TFile
from typing import Any, Callable, Dict, List, Tuple, Union
from clusterlib.executor import StageAbstract, Stage, StageInputFile, StageOutputFile, StageAbstractCollection_type, \
    MakeflowFromStages


HASH_STRING_BYTES_INT = 32


# ---------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------- PickleJobAbstract -------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
class PickleJobAbstract:
    def __init__(self, nr_cores_int: int, mem_MB_int: int):
        """
        Parameters
        ----------
        cores_int: int
            The number of CPU cores for the process.
        mem_MB_int: int
            The amount of mega-bytes of memory that is available for the process.
        """

        self.nr_cores_int = nr_cores_int
        self.mem_MB_int = mem_MB_int

    def get_stage(self):
        raise NotImplementedError()


# ---------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------- PickleVariable  --------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
class PickleVariable:
    def __init__(self, pickle_job_obj: PickleJobAbstract, pckl_parm_fileP_str: str, tpl_idx: Union[int, None] = None):
        """
        Parameters
        ----------
        pickle_job_obj: PickleJobAbstract
            The pickle job that create the output parameter associated with the pickle variable.
        pckl_parm_fileP_str: str
            The file location where output parameters are pickeled.
        tpl_idx: int
            The tuple output number that is selected.
        """

        self.pickle_job_obj = pickle_job_obj
        self.pckl_parm_fileP_str = pckl_parm_fileP_str
        self.tpl_idx = tpl_idx
        self.hash_key_str = None

    def _assert_hash_key(self):
        if self.hash_key_str is None:
            err_str = 'The hash key has to be set first before using the PickleVariable.'
            raise ValueError(err_str)

    def set_hash_key(self, hash_str: str = None):
        if self.hash_key_str is not None:
            err_str = 'The has key has already been set.'
            raise ValueError(err_str)

        if hash_str is None:
            hash_str = secrets.token_hex(HASH_STRING_BYTES_INT)

        self.hash_key_str = hash_str

    def get_hash_key(self):
        self._assert_hash_key()

        return self.hash_key_str

    def get_pickle_job_obj(self) -> PickleJobAbstract:
        self._assert_hash_key()

        return self.pickle_job_obj

    def get_fileP_str(self) -> str:
        self._assert_hash_key()

        return self.pckl_parm_fileP_str

    def get_tpl_index(self) -> Union[int, None]:
        self._assert_hash_key()

        return self.tpl_idx

    def __str__(self):
        return self.get_hash_key()

    @staticmethod
    def expand_pickle_variables(var_obj, stage_input_file_obj_dct, index_tuple_dct, depend_pickle_job_obj_lst):
        """For a given list, dictionary or object, if the object is a PickleVariable,
        set the has key and create a StageInputFile object and tuple mapping.

        TODO: Write up better documentation."""

        if isinstance(var_obj, PickleVariable) is True:
            var_obj.set_hash_key()
            hash_key_str = str(var_obj)

            if hash_key_str in stage_input_file_obj_dct:
                err_str = f'Random hash key "{hash_key_str}" is already present in stage input file dictionary.'
                raise ValueError(err_str)

            stage_input_file_obj_dct[hash_key_str] = StageInputFile(var_obj.get_fileP_str())
            index_tuple_dct[hash_key_str] = var_obj.get_tpl_index()
            depend_pickle_job_obj_lst.append(var_obj.get_pickle_job_obj())

        elif isinstance(var_obj, list) is True:
            for _var_obj in var_obj:
                PickleVariable.expand_pickle_variables(_var_obj,
                                                       stage_input_file_obj_dct,
                                                       index_tuple_dct,
                                                       depend_pickle_job_obj_lst)

        elif isinstance(var_obj, tuple) is True:
            for _var_obj in var_obj:
                PickleVariable.expand_pickle_variables(_var_obj,
                                                       stage_input_file_obj_dct,
                                                       index_tuple_dct,
                                                       depend_pickle_job_obj_lst)

        elif isinstance(var_obj, dict) is True:
            for _var_obj in var_obj.values():
                PickleVariable.expand_pickle_variables(_var_obj,
                                                       stage_input_file_obj_dct,
                                                       index_tuple_dct,
                                                       depend_pickle_job_obj_lst)

    @staticmethod
    def contract_pickle_variables(var_obj, stage_input_file_obj_dct, index_tuple_dct):
        """Opposite of `expand_pickle_variables`.

        TODO: Write up better documentation."""

        return_obj = None

        if isinstance(var_obj, PickleVariable) is True:
            # Get the has key
            hash_key_str = var_obj.get_hash_key()

            # Load the pickle file
            with open(stage_input_file_obj_dct[hash_key_str], 'rb') as file_obj:
                data_tpl = cloudpickle.load(file_obj)

            if index_tuple_dct[hash_key_str] is None:
                return_obj = data_tpl
            elif isinstance(data_tpl, tuple):
                return_obj = data_tpl[index_tuple_dct[hash_key_str]]
            else:
                return_obj = data_tpl

        elif isinstance(var_obj, list) is True:
            return_obj = []
            for _var_obj in var_obj:
                return_obj.append(PickleVariable.contract_pickle_variables(_var_obj,
                                                                           stage_input_file_obj_dct,
                                                                           index_tuple_dct))

        elif isinstance(var_obj, tuple) is True:
            return_obj = []
            for _var_obj in var_obj:
                return_obj.append(PickleVariable.contract_pickle_variables(_var_obj,
                                                                           stage_input_file_obj_dct,
                                                                           index_tuple_dct))
            return_obj = tuple(return_obj)

        elif isinstance(var_obj, dict) is True:
            return_obj = dict()
            for key_str, _var_obj in var_obj.items():
                return_obj[key_str] = PickleVariable.contract_pickle_variables(_var_obj,
                                                                               stage_input_file_obj_dct,
                                                                               index_tuple_dct)

        else:
            return_obj = var_obj

        return return_obj


# ---------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------- PickleJob -----------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
class PickleJobFilepathGenerator:
    """Generate file paths for the Pickle job class."""

    def __init__(self,
                 pickle_call_dirP_str: str,
                 pickle_call_kwargs_dirP_str: str,
                 pickle_out_dirP_str: str):
        """
        Parameters
        ----------
        pickle_call_dirP_str: str
            TODO
        pickle_call_kwargs_dirP_str: str
            TODO
        pickle_out_dirP_str: str
            TODO"""

        self.pickle_dirP_str = pickle_call_dirP_str
        self.pickle_call_kwargs_dirP_str = pickle_call_kwargs_dirP_str
        self.pickle_out_dirP_str = pickle_out_dirP_str

        # Keep track of the files path that are created to make sure there are no dupblicates
        self._name_group_tpl_dct: Dict[str, int] = dict()

    @staticmethod
    def _create_group_dirP_str(group_str_lst: Union[List[str], None]) -> str:
        if group_str_lst is not None:
            dirP_group_str = os.path.sep.join(group_str_lst)
        else:
            dirP_group_str = ''

        return dirP_group_str

    def _create_fileP(self, base_dirP_str: str, name_str: str, group_str_lst: Union[List[str], None] = None) -> str:
        # Create a unique name
        unique_name_str = self.create_unique_name(name_str, group_str_lst)

        # Create the full file path
        fileP_str = os.path.join(base_dirP_str, unique_name_str)

        # Make the necessary directory
        os.makedirs(os.path.dirname(fileP_str), exist_ok=True)

        return fileP_str

    def create_unique_name(self, name_str: str, group_str_lst: Union[List[str], None] = None) -> str:
        dirP_group_str = self._create_group_dirP_str(group_str_lst)
        pre_fileP_str = os.path.join(dirP_group_str, name_str)

        if pre_fileP_str in self._name_group_tpl_dct:
            number_int = self._name_group_tpl_dct[pre_fileP_str] + 1
            self._name_group_tpl_dct[pre_fileP_str] = number_int
        else:
            number_int = 0
            self._name_group_tpl_dct[pre_fileP_str] = number_int

        unique_name_str = f'{name_str}_{number_int}'

        return unique_name_str

    def create_pickle_call_fileP_str(self, name_str: str, group_str_lst: Union[List[str], None]) -> str:
        if self.pickle_dirP_str is None:
            err_str = 'The parameter pickle_call_dirP_str has not been set.'
            raise AssertionError(err_str)

        # Create the file path
        fileP_str = self._create_fileP(self.pickle_dirP_str,
                                       'call_' + name_str,
                                       group_str_lst) + '.p'

        return fileP_str

    def create_pickle_call_kwargs_fileP_str(self, name_str: str, group_str_lst: Union[List[str], None]) -> str:
        if self.pickle_call_kwargs_dirP_str is None:
            err_str = 'The parameter pickle_call_kwargs_dirP_str has not been set.'
            raise AssertionError(err_str)

        # Create the file path
        fileP_str = self._create_fileP(self.pickle_call_kwargs_dirP_str,
                                       'call_kwargs_' + name_str,
                                       group_str_lst) + '.p'

        return fileP_str

    def create_pickle_out_fileP_str(self, name_str: str, group_str_lst: Union[List[str], None]) -> str:
        if self.pickle_out_dirP_str is None:
            err_str = 'The parameter pickle_out_dirP_str has not been set.'
            raise AssertionError(err_str)

        # Create the file path
        fileP_str = self._create_fileP(self.pickle_out_dirP_str,
                                       'out_' + name_str,
                                       group_str_lst) + '.p'

        return fileP_str


# ---------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------- PickleJob -----------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
class BusyError(Exception):
    def __init__(self, err_str: str = ''):
        super(BusyError, self).__init__(err_str)


class PickleJob(PickleJobAbstract):
    def __init__(self,
                 name_str: str,
                 call_obj: Callable,
                 call_kwargs: Dict[str, Any],
                 fileP_gen_obj: PickleJobFilepathGenerator,
                 nr_cores_int: int = 1,
                 mem_MB_int: int = 1024,
                 group_str_lst: Union[List[str], None] = None):
        """

        Parameters
        ----------
        name_str: str
            The name of the pickled job.
        call_obj: Callable
            The function/method that will be called.
        call_kwargs: dict
            The keyword arguments that will be passed to the function/method.
        fileP_gen_obj: PickleJobFilepathGenerator
            File path generator.
        nr_cores_int: int
            The number of CPU cores for the process; default is 1.
        mem_MB_int: int
            The amount of mega-bytes of memory that is available for the process; default is 1 GB.
        group_str_lst: list of str
            TODO
        """

        super(PickleJob, self).__init__(nr_cores_int, mem_MB_int)

        # Create a unique name
        self.name_str = fileP_gen_obj.create_unique_name(name_str, group_str_lst)

        # Record the pickle file path and the output file path
        self._pickle_call_fileP_str = fileP_gen_obj.create_pickle_call_fileP_str(name_str, group_str_lst)
        self._pickle_call_kwargs_fileP_str = fileP_gen_obj.create_pickle_call_kwargs_fileP_str(name_str, group_str_lst)
        self._pickle_out_fileP_str = fileP_gen_obj.create_pickle_out_fileP_str(name_str, group_str_lst)

        # Record the callable object and its keyword arguments.
        self._call_obj = call_obj
        self._call_kwargs = call_kwargs

        # Check that the given call keyword arguments match the call_obj signature
        self.__check_input_parm_signature()
        self.__check_output_parm_signature()

        # Pickle the callable object
        self._pickle(self._pickle_call_fileP_str, self._call_obj)

        # Prepare the for the Stage object creation
        input_parm_obj_dct, depend_pickle_job_obj_lst = self.__create_stage_input_parm_obj_dct()
        self._Stage_kwargs_dct = {
            'name_str': self.name_str,
            'py_func': pickle_job_execute,
            'input_parm_obj_dct': input_parm_obj_dct,
            'output_file_dct': self.__create_stage_output_file_dct()
        }

        # Record the pickle jobs that this pickle job depends on
        if len(depend_pickle_job_obj_lst) == 0:
            self._depend_pickle_job_obj_lst: Union[List[PickleJobAbstract], None] = None
        else:
            self._depend_pickle_job_obj_lst: Union[List[PickleJobAbstract], None] = depend_pickle_job_obj_lst

        # Create the stage object place holder
        self._stage_obj: Union[Stage, None] = None

    def __check_input_parm_signature(self):
        """Check if the keyword arguments of the given callable object matches with the
        given keyword dictionary.

        Raises
        ------
        AssertionError:
            If keyword arguments of the callable object and given keyword dictionary do
            not match, an assertion error is raised.
        SyntaxError:
            If keyword type has not been set in the callable object.
        TypeError:
            If the type of the given keyword parameter is incorrect.

        TODO
        ----
        Add code that check that the types are the same between the keyword parameters.
        """

        # Check if the given input parameters are all present based on the input signature
        # of the call object
        sig_obj = signature(self._call_obj)

        # The set of parameters of the object that will be called
        sgn_call_parm_set = set(sig_obj.parameters.keys())
        # The set of parameters that were given
        gvn_call_parm_set = set(self._call_kwargs.keys())
        # The set of parameters that the object expects but are not present in the given
        # set of parameters
        missing_param_set = sgn_call_parm_set - gvn_call_parm_set
        # Check if there are any excess parameters
        excess_param_set = gvn_call_parm_set - sgn_call_parm_set

        if len(missing_param_set) > 0:
            err_str = f'The keyword parameters {str(missing_param_set)} are ' \
                + 'missing from parameter dictionary "call_kwargs"'
            raise AssertionError(err_str)

        if len(excess_param_set) > 0:
            err_str = f'The keyword parameters {str(excess_param_set)} are ' \
                + 'are not present given callable object "call_obj"'
            raise AssertionError(err_str)

        # # Check if types of the parameters matches
        # for parm_name_str, parameter_obj in sig_obj.parameters.items():
        #     if parameter_obj.annotation is sig_obj.empty:
        #         err_str = f"""The type for the parameter "{parm_name_str}" in the '""" \
        #             + f"""callable "{str(self._call_obj)}" has not been set."""
        #         raise SyntaxError(err_str)

        #     parm_call_kwargs_obj = self._call_kwargs[parm_name_str]
        #     if isinstance(parm_call_kwargs_obj, PickleVariable) is True:
        #         # TODO
        #         pass

        #     elif isinstance(parm_call_kwargs_obj, parameter_obj.annotation) is False:
        #         err_str = f'The keyword parameter {parm_name_str} has type {str(type(parm_call_kwargs_obj))} ' \
        #             + f'whereas in the callable object the corresponding type is {str(parameter_obj.annotation)}'
        #         raise TypeError(err_str)

    def __check_output_parm_signature(self):
        """Make sure that the callable object has an output signature.

        Raises
        ------
        SyntaxError:
            If keyword type has not been set in the return of the callable object.
        """

        sig_obj = signature(self._call_obj)
        if sig_obj.return_annotation is sig_obj.empty:
            err_str = f'The return of the the callable object {self._call_obj} is not annotated.'
            raise SyntaxError(err_str)

    def __create_stage_input_parm_obj_dct(self) -> Tuple[Dict[str, Any], List[PickleJobAbstract]]:
        """Create the parameter "input_parm_obj_dct" for the Stage class."""

        # Create the dictionary that that represents the parameter
        # "input_parm_obj_dct" for the Stage class. The function
        # that will be called is "pickle_job_execute"
        input_parm_obj_dct = dict()

        # The list of pickle jobs that this pickel job depends on
        depend_pickle_job_obj_lst: List[PickleJobAbstract] = []

        # Start to set the input parameters of the Stage class
        input_parm_obj_dct['pickle_call_fileP_str'] = StageInputFile(self._pickle_call_fileP_str)
        input_parm_obj_dct['pickle_call_kwargs_fileP_str'] = StageInputFile(self._pickle_call_kwargs_fileP_str)
        input_parm_obj_dct['pickle_out_fileP_str'] = StageOutputFile(self._pickle_out_fileP_str)
        input_parm_obj_dct['stage_input_file_obj_dct'] = dict()
        input_parm_obj_dct['index_tuple_dct'] = dict()

        input_kwargs_dct = copy.deepcopy(self._call_kwargs)
        PickleVariable.expand_pickle_variables(input_kwargs_dct,
                                               input_parm_obj_dct['stage_input_file_obj_dct'],
                                               input_parm_obj_dct['index_tuple_dct'],
                                               depend_pickle_job_obj_lst)

        self._pickle(self._pickle_call_kwargs_fileP_str, input_kwargs_dct)

        return input_parm_obj_dct, depend_pickle_job_obj_lst

    def __create_stage_output_file_dct(self) -> dict:
        """Create the parameter "output_file_dct" for the Stage class."""

        output_file_dct = {
            'output': StageOutputFile(self._pickle_out_fileP_str)
        }

        return output_file_dct

    @staticmethod
    def _pickle(pickle_fileP_str: str, pickle_obj: object):
        """Pickle the job dictionary."""

        with open(pickle_fileP_str, 'wb') as file_obj:
            cloudpickle.dump(pickle_obj, file_obj)

    def __getitem__(self, idx: int) -> PickleVariable:
        """Get output of the Pickled Job.

        Parameters
        ----------
        idx: int
            The output number of the Pickled Job that will be selected.

        Returns
        -------
        PickleVariable:
            The output as a Pickle Variable.
        """

        return self.get_pickle_variable(idx)

    def get_pickle_variable(self, idx: Union[int, None] = None) -> PickleVariable:
        """Get output of the Pickled Job.

        Parameters
        ----------
        idx: int
            The output number of the Pickled Job that will be selected.

        Returns
        -------
        PickleVariable:
            The output as a Pickle Variable.
        """

        if isinstance(idx, int) is True:
            # Count the number of output parameters of the callable object
            re_obj = re.compile(r'.*\[(?P<param>.*)\].*')
            sig_obj = signature(self._call_obj)
            match_obj = re_obj.match(str(sig_obj.return_annotation))

            if match_obj is not None:
                parameters_str = re_obj.match(str(sig_obj.return_annotation)).groupdict()['param']
                nr_output_param_int = parameters_str.count(',') + 1

                if idx > (nr_output_param_int - 1):
                    err_str = f'The index {idx} is out of range of the number of output ' \
                        + f'parameters of {nr_output_param_int}'
                    raise IndexError(err_str)
            else:
                if idx > 0:
                    err_str = f'The index {idx} is out of range of the number of output parameters 0'
                    raise IndexError(err_str)

        return PickleVariable(self, self._pickle_out_fileP_str, idx)

    def result(self):
        """Get the output result of the job."""

        # Check if the output file exists; if it does not exists, then throw a BusyError exception
        if os.path.exists(self._pickle_out_fileP_str) is False:
            err_str = f'Pickle Job {self.name_str} has not produced an output file yet.'
            raise BusyError(err_str)

        # Copy from remote file system
        with TFile.TFileFrom(fileP_str=self._pickle_out_fileP_str) as tfile_obj:
            with open(tfile_obj.local_fileP_str, 'rb') as file_obj:
                return_obj = cloudpickle.load(file_obj)

        return return_obj

    def create_stage(self,
                     graph_stage_dct: Dict[str, Tuple[StageAbstract, StageAbstractCollection_type]],
                     log_fileN_str: Union[str, None]):
        """Create a stage object.

        Parameters
        ----------
        graph_stage_dct: dict
            This dictionary keeps track of the stage graph.
        input_stage_obj: Stage
            The previous stage object or objects.
        log_fileN_str: str
            Name of the log file.

        Returns
        -------
        Stage:
            The stage object.

        Notes
        -----
        This function MUST only be called by the class PickleJarofJobs.
        """

        # Create the list of input stages that this stage depends on
        if self._depend_pickle_job_obj_lst is not None:
            input_stage_obj_lst = []
            for depend_pickle_job_obj in self._depend_pickle_job_obj_lst:
                input_stage_obj_lst.append(depend_pickle_job_obj.get_stage())
        else:
            input_stage_obj_lst = None

        # Create a cope of the stage keyword arguments
        stage_kwargs_dct = copy.deepcopy(self._Stage_kwargs_dct)

        # Complete the stage keyword arguments
        stage_kwargs_dct['graph_stage_dct'] = graph_stage_dct
        stage_kwargs_dct['log_fileN_str'] = log_fileN_str
        stage_kwargs_dct['input_stage_obj'] = input_stage_obj_lst

        # Create the stage
        self._stage_obj = Stage(**stage_kwargs_dct)

    def get_stage(self) -> Stage:
        if self._stage_obj is None:
            err_str = 'This job pickle object has not been passed to the method PickleJarofJobs.add.'
            raise ValueError(err_str)

        return self._stage_obj


# ---------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------- PickleJarOfJobs --------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
class PickleJarOfJobs:
    """A jar full of pickle jobs to be executed."""

    def __init__(self, jar_name_str: str, pickle_jar_dirP_str: str):
        """

        Parameters
        ----------
        jar_name_str: str
            The name of the pickle jar.
        pickle_jar_dirP_str: str
            The directory location where to archive the support files of the job pickel files.
        """

        self.jar_name_str = jar_name_str
        self.pickle_jar_dirP_str = pickle_jar_dirP_str

        self._graph_stage_dct = dict()

    def add(self, pickle_job_obj: PickleJob):
        """Add a pickle job to the pickle jar.

        Parameters
        ----------
        pickle_job_obj: PickleJob
            The pickle job object.
        """

        log_dirP_str = os.path.join(self.pickle_jar_dirP_str, self.jar_name_str, 'logging')
        os.makedirs(log_dirP_str, exist_ok=True)

        log_fileN_str = os.path.join(log_dirP_str, pickle_job_obj.name_str) + '.log'
        pickle_job_obj.create_stage(self._graph_stage_dct, log_fileN_str)

    def get_makeflow_base_dirP(self) -> str:
        base_dirP_str = os.path.join(self.pickle_jar_dirP_str, self.jar_name_str, 'makeflow')

        return base_dirP_str

    def create_makeflow_stages(self,
                               wrapper_bash_scrpt_fileP_str: str,
                               yaml_cfg_dct: dict = None) -> MakeflowFromStages:
        """Create the makeflow file.

        Parameters
        ----------
        wrapper_bash_scrpt_fileP_str: str
            The bash wrapper script executable for the python script.
        yaml_cfg_dct: dict
            Makeflow command options. For example,
            {
                'command_options': {
                    'shared_fs': {
                        'option': '--shared-fs',
                        'value': 'bla bla'
                    }
                }
            }
        """

        base_dirP_str = self.get_makeflow_base_dirP()
        os.makedirs(base_dirP_str, exist_ok=True)

        def _makedirs(_dirP_str: str) -> str:
            os.makedirs(_dirP_str, exist_ok=True)

            return _dirP_str

        # Create the MakeflowFromStages object
        MakeflowFromStages_kwargs_dct = {
            'parm_dirP_str': _makedirs(os.path.join(base_dirP_str, 'parameters')),
            'wrapper_bash_scrpt_fileP_str': wrapper_bash_scrpt_fileP_str,
            'graph_stage_dct': self._graph_stage_dct,
            'makeflow_out_fileP_str': os.path.join(base_dirP_str, f'{self.jar_name_str}.makeflow'),
            'py_caller_script_fileP_str': os.path.join(base_dirP_str, f'{self.jar_name_str}_caller.py'),
            'yaml_cfg_dct': yaml_cfg_dct
        }

        makeflow_obj = MakeflowFromStages(**MakeflowFromStages_kwargs_dct)

        return makeflow_obj


# ---------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------ pickle_job_execute  ------------------------------------------------
# -------------------------------------------------       BEGIN       -------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
def pickle_job_execute(pickle_call_fileP_str: str,
                       pickle_call_kwargs_fileP_str: str,
                       pickle_out_fileP_str: str,
                       stage_input_file_obj_dct: Union[Dict[str, str], None] = None,
                       index_tuple_dct: Union[Dict[str, int], None] = None):
    """Execute a pickled job.

    Parameters
    ----------
    pickle_call_fileP_str: str
        The file path to the pickeled function/class that will be called.
    pickle_call_kwargs_fileP_str: dict
        TODO
    pickle_out_fileP_str: str
        The output file path of where to pickle the results.
    stage_input_file_obj_dct: list of tuples
        TODO The file paths to the outputs of other pickled jobs, which is optional.
    index_tuple_dct: tuple
        TODO Each list element MUST consists of a two element tuple, where each tuple
        corresponding to to each file path listed in the parameter
        "input_pckl_fileP_str"; there MUST be a one-to-one mapping. The first
        tuple element is the tuple element that should be selected from the pickeled
        file and the last tuple element is the keyword name that should be used to
        pass the selected tuple element. If the first tuple element is None,
        then the whole variable in the pickle fill will be passed via the
        keyword name.
    """

    # Load the pickled callable object
    with open(pickle_call_fileP_str, 'rb') as file_obj:
        call_obj = cloudpickle.load(file_obj)

    # Load the pickled kwargs dictionary for the callable object
    with open(pickle_call_kwargs_fileP_str, 'rb') as file_obj:
        call_input_kwargs_dct = cloudpickle.load(file_obj)

    # Replace the PickleVariable object inside of "call_input_kwargs_dct" with loaded pickled values
    call_input_kwargs_dct = PickleVariable.contract_pickle_variables(call_input_kwargs_dct,
                                                                     stage_input_file_obj_dct,
                                                                     index_tuple_dct)

    # Call the callable object and get the output
    output_tpl = call_obj(**call_input_kwargs_dct)

    # Pickle the output
    with open(pickle_out_fileP_str, 'wb') as file_obj:
        cloudpickle.dump(output_tpl, file_obj)
