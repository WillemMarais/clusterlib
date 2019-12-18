import os
import h5py
import socket
import logging
import tempfile
import subprocess
import numpy as np
from typing import Dict, Tuple
from yaml import load, FullLoader
from clusterlib.file import get_scratch_dirP

log_obj = logging.getLogger(__name__)


class matlabBinary:
    """Call a matlab binary which is a compiled function."""

    def __init__(self, mtlb_bin_fileP_str: str, tmp_dirP_str: str = None):
        """
        Parmeters
        ---------
        mtlb_bin_fileP_str: str
            The matlab binary file path.
        tmp_dirP_str: str
            The temporary directory where to store intermediate files.
        """

        # Set the matlab binary path
        if os.path.exists(mtlb_bin_fileP_str) is False:
            err_str = f'The matlab binary file "{mtlb_bin_fileP_str}" does not exists.'
            raise FileNotFoundError(err_str)
        self.mtlb_bin_fileP_str = mtlb_bin_fileP_str

        # Save the temporary directory
        if tmp_dirP_str is None:
            self.tmp_dirP_str = os.path.join(get_scratch_dirP(add_pid_bl=False),
                                             str(os.path.basename(self.mtlb_bin_fileP_str)))
            os.makedirs(self.tmp_dirP_str, exist_ok=True)
        else:
            self.tmp_dirP_str = tmp_dirP_str

        # ----------------------------------------------------------------------
        # Create the necessay attributes for the execution of the Matlab binary

        # Set the path to the config file
        self.cnf_fileP_str = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          'config',
                                          'matlab.yaml')

        if os.path.exists(self.cnf_fileP_str) is False:
            err_str = f'Could not find the local config file "{self.cnf_fileP_str}"'
            raise FileNotFoundError(err_str)

        # Load the config file
        yaml_cfg_dct = self._load_config()

        # Get Matlab version
        host_name_str = socket.gethostname()
        if host_name_str not in yaml_cfg_dct['matlab']:
            matlab_version_str = yaml_cfg_dct['matlab']['default']['version']
            matlab_dirP_str = os.path.join(yaml_cfg_dct['matlab']['default']['location'],
                                           matlab_version_str)
        else:
            matlab_version_str = yaml_cfg_dct['matlab'][host_name_str]['version']
            matlab_dirP_str = os.path.join(yaml_cfg_dct['matlab'][host_name_str]['location'],
                                           matlab_version_str)

        # Create the environmental variables for the Matlab binary execution
        env_ld_lib_path_str = os.path.join(matlab_dirP_str, 'runtime/glnxa64')
        env_ld_lib_path_str += ':' + os.path.join(matlab_dirP_str, 'bin/glnxa64')
        env_ld_lib_path_str += ':' + os.path.join(matlab_dirP_str, 'sys/os/glnxa64')
        env_ld_lib_path_str += ':' + os.path.join(matlab_dirP_str, 'sys/opengl/lib/glnxa64')

        # Set the Matlab environmental variable dictionary
        self.mtlb_env_dct = {
            'LD_LIBRARY_PATH': env_ld_lib_path_str
        }

    def _load_config(self) -> dict:
        """Load the YAML config file and return as dictionary.

        Returns
        -------
        yaml_cfg_dct: dict
            The YAML configuration as a dictionary.
        """

        if self.cnf_fileP_str is None:
            yaml_cfg_dct = dict()
        else:
            with open(self.cnf_fileP_str, 'r') as file_obj:
                yaml_cfg_dct = load(file_obj, Loader=FullLoader)

        return yaml_cfg_dct

    @staticmethod
    def _write_parameters_to_h5(fileP_str: str, param_dct: Dict[str, np.ndarray]):
        with h5py.File(fileP_str, 'w') as h5_file_obj:
            for ds_name_str, data_arr in param_dct.items():
                h5_file_obj.create_dataset(ds_name_str, data=data_arr)

    @staticmethod
    def _read_results_from_h5(fileP_str: str) -> Dict[str, np.ndarray]:
        return_dct = dict()
        with h5py.File(fileP_str, 'r') as h5_file_obj:
            for ds_name_str, ds_h5_obj in h5_file_obj.items():
                return_dct[ds_name_str] = ds_h5_obj[:]

        return return_dct

    def execute(self,
                param_dct: Dict[str, np.ndarray],
                timeout_sec_int: int = int(2.5 * 60 * 60)) -> Tuple[Dict[str, np.ndarray], str, str]:
        """Execute the matlab binary.

        Parameters
        ----------
        param_dct: str
            The input parameters.
        timeout_sec_int: int
            Execution time out.

        Returns
        -------
        result_dct: dict
            TODO
        std_out_str: str
            TODO
        std_err_str: str
            TODO
        """

        base_fileN_str = str(os.path.basename(self.mtlb_bin_fileP_str))
        with tempfile.TemporaryDirectory(dir=self.tmp_dirP_str) as tmp_dirP_str:
            # Create the input and output file paths
            input_fileP_str = os.path.join(str(tmp_dirP_str), f'input_file_{base_fileN_str}.h5')
            output_fileP_str = os.path.join(str(tmp_dirP_str), f'output_file_{base_fileN_str}.h5')
            os.makedirs(os.path.dirname(input_fileP_str), exist_ok=True)

            # Write the input parameters to the appropriate file
            self._write_parameters_to_h5(input_fileP_str, param_dct)

            # Execute the matlab binary
            arg_str_lst = [
                self.mtlb_bin_fileP_str,
                input_fileP_str,
                output_fileP_str
            ]

            p_obj = subprocess.Popen(arg_str_lst,
                                     env=self.mtlb_env_dct,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            p_obj.wait(timeout=timeout_sec_int)
            return_code_int = p_obj.returncode
            if return_code_int > 0:
                err_str = 'The matlab binary {:s}  with input arguments {:s} failed with exit ' \
                    + 'code {:d}\nstdout: {:s}\nstderr: {:s}.'
                raise RuntimeError(err_str.format(self.mtlb_bin_fileP_str,
                                                  str(arg_str_lst),
                                                  return_code_int,
                                                  str(p_obj.stdout.read()),
                                                  str(p_obj.stderr.read())))

            # Read the results
            result_dct = self._read_results_from_h5(output_fileP_str)

        std_out_str = str(p_obj.stdout.read())
        std_err_str = str(p_obj.stderr.read())

        return result_dct, std_out_str, std_err_str
