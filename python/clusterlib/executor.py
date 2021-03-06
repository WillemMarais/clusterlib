import os
import stat
import copy
import yaml
import inspect
import secrets
import importlib
import clusterlib.file as Tfile
import clusterlib.makeflow as makeflow
from clusterlib.utilities import flatten_list_dict
from typing import Callable, Dict, List, Tuple, Union


HASH_STRING_LENGTH_INT = 32


class StageFile:
    def __init__(self, fileP_str: str):
        self.fileP_str = fileP_str

    def __str__(self):
        raise NotImplementedError()


class StageInputFile(StageFile):
    def __init__(self, fileP_str: str):
        super(StageInputFile, self).__init__(fileP_str)

    def __str__(self):
        return self.fileP_str


class StageOutputFile(StageFile):
    def __init__(self, fileP_str: str):
        super(StageOutputFile, self).__init__(fileP_str)

    def __str__(self):
        return self.fileP_str


class StageAbstract:
    def __init__(self, name_str: str):
        self.name_str = name_str

    def crt_makeflow_rule(self, cat_obj: makeflow.Category, parm_dir_fileP_str: str,
                          wrapper_bash_scrpt_fileP_str: str) -> makeflow.Rule:
        """Create a makeflow rule.

        Parameters
        ----------
        cat_obj: makeflow.Category
            A Makeflow category object.
        parm_dir_fileP_str: str
            The file path of the directory for a file which will contain the parameters
            of the function that will be executed.
        wrapper_bash_scrpt_fileP_str: str
            The wrapper bash script that will call the corresponding stage.

        Returns
        -------
        makeflow.Category:
            The makeflow rule."""

        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError()


StageAbstractCollection_type = Union[
    StageAbstract,
    List[StageAbstract],
    Tuple[StageAbstract],
    Dict[str, StageAbstract],
    None
]


class Stage(StageAbstract):
    """Execution stage."""

    def __init__(self,
                 input_stage_obj: StageAbstractCollection_type,
                 graph_stage_dct: Dict[str, Tuple[StageAbstract, StageAbstractCollection_type]],
                 name_str: str,
                 py_func: Callable,
                 input_parm_obj_dct: Dict[str, Union[int, float, str, StageFile, List[StageFile]]],
                 output_file_dct: Dict[str, StageFile],
                 log_fileN_str: str,
                 nr_cores_int: int = 1,
                 mem_MB_int: int = 1024):
        """

        Parameter
        ---------
        input_stage_obj: StageAbstractCollection_type
            The previous stage object or objects.
        graph_stage_dct: dict of StageAbstract
            This dictionary keeps track of the stage graph.
        name_str: str
            The name of this stage.
        py_func: Callable
            The python function that will be called.
        input_parm_obj_dct: dictionary of either int, float or str
            The named input parameters to the python function that will be called.
        output_fileP_str_dct: dict of str
            List of named output files for the stage.
        log_fileN_str: str
            Name of the log file.
        nr_cores_int: int
            The number of CPU cores for the process; default is 1.
        mem_MB_int: int
            The amount of mega-bytes of memory that is available for the process; default is 1 GB.
        """

        if inspect.isfunction(py_func) is False:
            err_str = 'The parameter "py_func" has to be a function.'
            raise ValueError(err_str)

        super(Stage, self).__init__(name_str)

        self.py_func = py_func
        self.input_parm_obj_dct = input_parm_obj_dct
        self.output_file_dct = output_file_dct
        self.log_fileN_str = log_fileN_str
        self.nr_cores_int = nr_cores_int
        self.mem_MB_int = mem_MB_int

        # Make sure that the stage does not already exists in the graph
        if name_str in graph_stage_dct:
            err_str = f'A stage with name "{name_str}" already exists in the graph.'
            raise ValueError(err_str)

        def _check_stage_present(_stage_obj):
            if _stage_obj.name_str not in graph_stage_dct:
                err_str = f'The stage with name "{_stage_obj.name_str}" does not exists in the graph.'
                raise ValueError(err_str)

        # Make sure that the input stages already exist in the graph
        if input_stage_obj is not None:
            if isinstance(input_stage_obj, StageAbstract) is True:
                _check_stage_present(input_stage_obj)

            elif isinstance(input_stage_obj, dict) is True:
                for _input_stage_obj in input_stage_obj.values():
                    _check_stage_present(_input_stage_obj)

            elif (isinstance(input_stage_obj, list) is True) or (isinstance(input_stage_obj, tuple) is True):
                for _input_stage_obj in input_stage_obj:
                    _check_stage_present(_input_stage_obj)

            else:
                raise NotImplementedError()

            graph_stage_dct[name_str] = (self, input_stage_obj)

        else:
            graph_stage_dct[name_str] = (self, None)

    def crt_makeflow_rule(self,
                          parm_dirP_fileP_str: str,
                          py_caller_script_fileP_str: str,
                          wrapper_bash_scrpt_fileP_str: str,
                          cat_obj: makeflow.Category = None) -> makeflow.Rule:
        """Create a makeflow rule.

        Parameters
        ----------
        parm_dirP_fileP_str: str
            The file path of the directory for a file which will contain the parameters
            of the function that will be executed.
        py_caller_script_fileP_str: str
            The file path location where the python wrapper script is stored.
        wrapper_bash_scrpt_fileP_str: str
            The wrapper bash script that will call the corresponding stage.
        cat_obj: makeflow.Category
            A Makeflow category object; optional.

        Returns
        -------
        makeflow.Category:
            The makeflow rule."""

        if os.path.isdir(parm_dirP_fileP_str) is True:
            # parm_fileP_str = os.path.join(parm_dirP_fileP_str,
            #                               secrets.token_urlsafe(HASH_STRING_LENGTH_INT))
            parm_fileP_str = os.path.join(parm_dirP_fileP_str,
                                          f'{self.name_str}_parameters.yaml')
        else:
            parm_fileP_str = parm_dirP_fileP_str

        # Create the parameter dictionary
        parm_dct = {
            'module_path': self.py_func.__module__,
            'function': self.py_func.__name__,
            'function_kwargs': dict(),
            'input_fileP_str_dct': dict(),
            'output_fileP_str_dct': dict()
        }

        def transform_StageFile_objects(input_name_str, input_obj):
            _output_obj = None
            _input_fileP_str_obj = None
            _output_fileP_str_obj = None

            if isinstance(input_obj, StageInputFile) is True:
                _output_obj = str(input_obj)
                _input_fileP_str_obj = str(input_obj)

            elif isinstance(input_obj, StageOutputFile) is True:
                _output_obj = str(input_obj)
                _output_fileP_str_obj = str(input_obj)

            elif isinstance(input_obj, list) is True:
                _output_obj = list()
                _input_fileP_str_obj = list()
                _output_fileP_str_obj = list()
                for idx, _input_obj in enumerate(input_obj):
                    _input_name_str = f'{input_name_str}_{idx}'
                    __output_obj, __input_fileP_str_obj, __output_fileP_str_obj = \
                        transform_StageFile_objects(_input_name_str, _input_obj)

                    _output_obj.append(__output_obj)

                    if __input_fileP_str_obj is not None:
                        _input_fileP_str_obj.append(__input_fileP_str_obj)

                    if __output_fileP_str_obj is not None:
                        _output_fileP_str_obj.append(__output_fileP_str_obj)

                if len(_input_fileP_str_obj) == 0:
                    _input_fileP_str_obj = None

                if len(_output_fileP_str_obj) == 0:
                    _output_fileP_str_obj = None

            elif isinstance(input_obj, dict) is True:
                _output_obj = dict()
                _input_fileP_str_obj = dict()
                _output_fileP_str_obj = dict()
                for key_str, _input_obj in input_obj.items():
                    _input_name_str = f'{input_name_str}_{key_str}'
                    __output_obj, __input_fileP_str_obj, __output_fileP_str_obj = \
                        transform_StageFile_objects(_input_name_str, _input_obj)

                    _output_obj[key_str] = __output_obj

                    if __input_fileP_str_obj is not None:
                        _input_fileP_str_obj[key_str] = __input_fileP_str_obj

                    if __output_fileP_str_obj is not None:
                        _input_fileP_str_obj[key_str] = __output_fileP_str_obj

                if len(_input_fileP_str_obj) == 0:
                    _input_fileP_str_obj = None

                if len(_output_fileP_str_obj) == 0:
                    _output_fileP_str_obj = None

            else:
                _output_obj = input_obj

            return _output_obj, _input_fileP_str_obj, _output_fileP_str_obj

        # Populate the keyword arguments of the function; also keep a list of input files
        for input_parm_name_str, input_parm_value_obj in self.input_parm_obj_dct.items():
            output_obj, input_fileP_str_obj, output_fileP_str_obj = transform_StageFile_objects(input_parm_name_str,
                                                                                                input_parm_value_obj)

            parm_dct['function_kwargs'][input_parm_name_str] = output_obj
            if input_fileP_str_obj is not None:
                parm_dct['input_fileP_str_dct'][input_parm_name_str] = input_fileP_str_obj
            if output_fileP_str_obj is not None:
                parm_dct['output_fileP_str_dct'][input_parm_name_str] = output_fileP_str_obj

        # Write out the parameter yaml file
        with Tfile.TFileTo(fileP_str=parm_fileP_str) as tfile_obj:
            with open(tfile_obj.local_fileP_str, 'w') as file_obj:
                yaml.dump(parm_dct, stream=file_obj)

        # Make a list of output files that are not present in self.output_file_dct
        if self.output_file_dct is not None:
            for name_str, stage_file_obj in self.output_file_dct.items():
                parm_dct['output_fileP_str_dct'][name_str] = str(stage_file_obj)

        # Create the python wrapper script
        with open(py_caller_script_fileP_str, 'w') as file_obj:
            file_obj.write('import sys\n')
            file_obj.write('import clusterlib.executor as executor\n')
            file_obj.write('executor.Stage.execute(sys.argv[1])\n')

        # Create the command string
        cmd_str = f'/bin/bash {wrapper_bash_scrpt_fileP_str} {py_caller_script_fileP_str}' \
            + f' {parm_fileP_str} > {self.log_fileN_str} 2>&1'

        # Create the input and output file lists
        input_fileP_str_lst = [parm_fileP_str]
        if parm_dct['input_fileP_str_dct'] is not None:
            input_fileP_str_lst += flatten_list_dict(parm_dct['input_fileP_str_dct'])

        output_fileP_str_lst = []
        if parm_dct['output_fileP_str_dct'] is not None:
            output_fileP_str_lst += flatten_list_dict(parm_dct['output_fileP_str_dct'])

        # All of the items in `input_fileP_str_lst` should be strings
        for input_fileP_str in input_fileP_str_lst:
            if isinstance(input_fileP_str, str) is False:
                err_str = 'Not all the items of `input_fileP_str` are strings:\nFrom {:s} to {:s}'
                raise ValueError(err_str.format(str(parm_dct['input_fileP_str_dct']),
                                                str(input_fileP_str_lst)))

        if cat_obj is None:
            cat_obj = makeflow.Category(category_name_str=self.name_str,
                                        cores_int=self.nr_cores_int,
                                        mem_MB_int=self.mem_MB_int)

        # Create the makeflow rule object
        mf_rule_obj = makeflow.Rule(category_obj=cat_obj)
        mf_rule_obj.set_command(cmd_str,
                                input_fileP_str_lst,
                                output_fileP_str_lst)

        return mf_rule_obj

    @staticmethod
    def execute(parm_fileP_str: str):
        # Load the yaml parameter config file
        param_dct = None
        with Tfile.TFileFrom(fileP_str=parm_fileP_str) as tfile_obj:
            with open(tfile_obj.local_fileP_str, 'r') as file_obj:
                param_dct = yaml.load(stream=file_obj, Loader=yaml.FullLoader)

        # Load the appropriate module
        module_obj = importlib.import_module(param_dct['module_path'])

        # Get the function object
        func_obj = getattr(module_obj, param_dct['function'])

        # Create the yaml parameter config file that has the local file paths
        local_kwargs_param_dct = copy.deepcopy(param_dct['function_kwargs'])

        def tfile_from_flatten_list_dict(_itr_obj):
            _tfile_obj_lst = []

            if isinstance(_itr_obj, dict) is True:
                _rtn_itr_obj = dict()
                for _key_str, _value_obj in _itr_obj.items():
                    _new_value_obj, __tfile_obj_lst = \
                        tfile_from_flatten_list_dict(_value_obj)

                    _rtn_itr_obj[_key_str] = _new_value_obj
                    _tfile_obj_lst += __tfile_obj_lst

                return _rtn_itr_obj, _tfile_obj_lst

            elif isinstance(_itr_obj, list) is True:
                _rtn_itr_obj = list()
                for __itr_obj in _itr_obj:
                    _new_itr_obj, __tfile_obj_lst = \
                        tfile_from_flatten_list_dict(__itr_obj)

                    _rtn_itr_obj.append(_new_itr_obj)
                    _tfile_obj_lst += __tfile_obj_lst

                return _rtn_itr_obj, _tfile_obj_lst

            elif isinstance(_itr_obj, str) is True:
                _tfile_obj = Tfile.TFileFrom(fileP_str=_itr_obj)

                _tfile_obj_lst = [_tfile_obj]
                _rtn_itr_obj = _tfile_obj.local_fileP_str

                return _rtn_itr_obj, _tfile_obj_lst

            else:
                err_str = 'The given object has to be either a str, lst or dict.'
                raise ValueError(err_str)

        input_fileP_str_dct, in_tfile_obj_lst = tfile_from_flatten_list_dict(param_dct['input_fileP_str_dct'])
        for key_str, value_obj in input_fileP_str_dct.items():
            local_kwargs_param_dct[key_str] = value_obj

        # Create a list of transcended output files
        out_tfile_obj_lst = []
        for name_str, output_fileP_str in param_dct['output_fileP_str_dct'].items():
            tfile_obj = Tfile.TFileTo(fileP_str=output_fileP_str)
            out_tfile_obj_lst.append(tfile_obj)

            if name_str in local_kwargs_param_dct:
                local_kwargs_param_dct[name_str] = tfile_obj.local_fileP_str

        # Call the function
        with Tfile.TFileCollection(in_tfile_obj_lst):
            with Tfile.TFileCollection(out_tfile_obj_lst):
                func_obj(**local_kwargs_param_dct)


class MakeflowFromStages:
    """Create the makeflow file from the collection of stages."""

    def __init__(self,
                 parm_dirP_str: str,
                 wrapper_bash_scrpt_fileP_str: str,
                 graph_stage_dct: Dict[str, Tuple[StageAbstract, StageAbstractCollection_type]],
                 makeflow_out_fileP_str: str,
                 py_caller_script_fileP_str: str,
                 cat_obj: Union[makeflow.Category, None] = None,
                 yaml_cfg_dct: dict = None):
        """

        Parameters
        ----------
        parm_dirP_str: str
            The parameter directory path of where to archive the parameter YAML files for the stage execution.
        wrapper_bash_scrpt_fileP_str: str
            The iris bash wrapper script executable for the python script.
        graph_stage_dct: Dict[str, Tuple[StageAbstract, StageAbstractCollection_type]]
            The graph of the stages.
        makeflow_out_fileP_str: str
            The output file path of the makeflow file.
        py_caller_script_fileP_str: str
            The file path location where the python wrapper script is stored.
        cat_obj: makeflow.Category
            A Makeflow category object; optional.
        yaml_cfg_dct: dict
            Eg. {
                'command_options': {
                    'shared_fs': {
                        'option': '--shared-fs',
                        'value': 'bla bla'
                    }
                }
            }
        """

        self.parm_dirP_str_lst: List[str] = [parm_dirP_str]
        self.wrapper_bash_scrpt_fileP_str_lst: List[str] = [wrapper_bash_scrpt_fileP_str]
        self.cat_obj_lst: Union[List[Union[makeflow.Category, None]]] = [cat_obj]
        self.graph_stage_dct_lst: List[Dict[str, Tuple[StageAbstract, StageAbstractCollection_type]]] = [graph_stage_dct]
        self.makeflow_out_fileP_str_lst: List[str] = [makeflow_out_fileP_str]
        self.py_caller_script_fileP_str_lst: List[str] = [py_caller_script_fileP_str]

        if yaml_cfg_dct is None:
            self.yaml_cfg_dct_lst: List[Union[dict, None]] = [dict()]
        else:
            self.yaml_cfg_dct_lst: List[Union[dict, None]] = [yaml_cfg_dct]

    def extend(self, makeflow_stages_obj):
        self.parm_dirP_str_lst.extend(makeflow_stages_obj.parm_dirP_str_lst)
        self.wrapper_bash_scrpt_fileP_str_lst.extend(makeflow_stages_obj.wrapper_bash_scrpt_fileP_str_lst)
        self.cat_obj_lst.extend(makeflow_stages_obj.cat_obj_lst)

        self.graph_stage_dct_lst.extend(makeflow_stages_obj.graph_stage_dct_lst)
        self.makeflow_out_fileP_str_lst.extend(makeflow_stages_obj.makeflow_out_fileP_str_lst)
        self.py_caller_script_fileP_str_lst.extend(makeflow_stages_obj.py_caller_script_fileP_str_lst)
        self.yaml_cfg_dct_lst.extend(makeflow_stages_obj.yaml_cfg_dct_lst)

    def create(self, makeflow_out_fileP_str: str = None):
        """Create the Makeflow JX file.

        Parameters
        ----------
        makeflow_out_fileP_str: str
            The output file path of the makeflow file."""

        if (makeflow_out_fileP_str is None) and (len(self.makeflow_out_fileP_str_lst) == 1):
            makeflow_out_fileP_str = self.makeflow_out_fileP_str_lst[0]

        elif makeflow_out_fileP_str is None:
            err_str = 'Since multiple MakeflowFromStages objects have been combined, an output file has to be given'
            raise ValueError(err_str)

        # Create the makeflow JX file creator
        makeflow_jx_creator_obj = makeflow.JxMakeflow()

        for parm_dirP_str, wrapper_bash_scrpt_fileP_str, cat_obj, graph_stage_dct, \
            py_caller_script_fileP_str in zip(self.parm_dirP_str_lst,
                                              self.wrapper_bash_scrpt_fileP_str_lst,
                                              self.cat_obj_lst,
                                              self.graph_stage_dct_lst,
                                              self.py_caller_script_fileP_str_lst):

            # Create the keyword dictionary for the makeflow rule function
            kwargs_dct = {
                'parm_dirP_fileP_str': parm_dirP_str,
                'py_caller_script_fileP_str': py_caller_script_fileP_str,
                'wrapper_bash_scrpt_fileP_str': wrapper_bash_scrpt_fileP_str,
                'cat_obj': cat_obj
            }

            makeflow_jx_creator_obj.add_category(cat_obj)

            # Add the rules to the makeflow JX file creator
            for stage_obj, _ in graph_stage_dct.values():
                makeflow_jx_creator_obj.add_rule(stage_obj.crt_makeflow_rule(**kwargs_dct))

        # Write out the makeflow file
        os.makedirs(os.path.dirname(makeflow_out_fileP_str), exist_ok=True)
        with open(makeflow_out_fileP_str, 'w') as file_obj:
            file_obj.write(makeflow_jx_creator_obj.get_str())

        # Write the bash script thats execute the makeflow file
        bash_makeflow_fileP_str = os.path.join(os.path.dirname(makeflow_out_fileP_str),
                                               'run_' + os.path.basename(makeflow_out_fileP_str) + '.bash')
        self._create_makeflow_bash_command(makeflow_out_fileP_str,
                                           bash_makeflow_fileP_str)

        # Make the makeflow bash script executable
        st_obj = os.stat(bash_makeflow_fileP_str)
        os.chmod(bash_makeflow_fileP_str, st_obj.st_mode | stat.S_IXUSR)

    def _get_command_options(self) -> dict:
        return_dct = dict()

        for yaml_cfg_dct in self.yaml_cfg_dct_lst:
            if 'command_options' in yaml_cfg_dct:
                return_dct.update(yaml_cfg_dct['command_options'])

        return return_dct

    def _create_makeflow_bash_command(self, makeflow_fileP_str: str, out_fileP_str: str):
        out_str = """#!/usr/bin/env bash

makeflow"""

        # Add makeflow options
        command_options_dct = self._get_command_options()
        for _, option_value_dct in command_options_dct.items():
            option_str = option_value_dct['option']
            value_str = option_value_dct['value']
            out_str += f' {option_str} "{value_str}"'

        out_str += f' --jx {makeflow_fileP_str}\n'

        with open(out_fileP_str, 'w') as file_obj:
            file_obj.write(out_str)
