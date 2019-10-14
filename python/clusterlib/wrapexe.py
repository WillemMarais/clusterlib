import os
import yaml
import jinja2
import shutil
import tarfile
import tempfile
from collections import namedtuple
from typing import Dict, List, Tuple, Union
from clusterlib.utilities import get_dirP_of__file__


class PackageManifest:
    """Specification of a package manifest; this class is to be used with the class
    IrisWrapperExecute."""

    def __init__(self, name_str: str = None, src_dirP_str: str = None,
                 dst_tar_fileP_str: str = None, dst_dirP_str: str = None,
                 checksum_fileP_str: str = None):
        """

        Parameters
        ----------
        name_str: str
            The name of the package manifest; this will be used in the bash wrapper script.
        src_fileP_str: str
            The source directory of the package that will be tarred. If this is None, then
            then it is assumed that `dst_tar_fileP_str` is already pointing to a tarred package.
        dst_tar_fileP_str: str
            The destination file path of the tarred package. If  `src_fileP_str` is None, then
            it it is assumed that `dst_tar_fileP_str` is already pointing to a tarred package.
        dst_dirP_str: str
            The directory path of wherein the tar package is extracted.
        checksum_dirP_str: str
            The file path to where the wrapper script should write the MD5 check sum.
        """

        if name_str is not None:
            self.name_str = name_str.upper()
        else:
            self.name_str = 'None'

        if (src_dirP_str is None) and (os.path.exists(dst_tar_fileP_str) is False):
            err_str = 'For src_fileP_str=None, could not find the ' \
                + f'distination file path "{dst_tar_fileP_str}"'

            raise FileNotFoundError(err_str)
        # elif os.path.exists(src_dirP_str) is False:
        #     err_str = 'The directory "src_dirP_str" does not exists.'

        #     raise FileNotFoundError(err_str)

        self.src_dirP_str = src_dirP_str
        self.dst_tar_fileP_str = dst_tar_fileP_str
        self.dst_dirP_str = dst_dirP_str
        self.checksum_fileP_str = checksum_fileP_str

    def package(self, tmp_dirP_str: str = None):
        """Create the python package."""

        if self.src_dirP_str is None:
            return

        with tempfile.TemporaryDirectory(dir=tmp_dirP_str) as tmp_dir_obj:
            # Copy ther python package directory to the temporary directory
            out_dirN_str = os.path.basename(self.src_dirP_str)
            out_dirP_str = os.path.join(str(tmp_dir_obj), out_dirN_str)
            shutil.copytree(self.src_dirP_str, out_dirP_str, ignore=shutil.ignore_patterns('.git'))

            # Change directory to the temporary directory
            current_dirP_str = os.getcwd()
            os.chdir(str(tmp_dir_obj))

            # Tar the directory
            try:
                tmp_tar_fileP_str = os.path.join(str(tmp_dir_obj), out_dirN_str + '.tar')
                with tarfile.open(tmp_tar_fileP_str, 'w') as tar_obj:
                    tar_obj.add(out_dirN_str)

                # Copy the tar file to the destination file path
                shutil.copyfile(tmp_tar_fileP_str, self.dst_tar_fileP_str)

            finally:
                # Change back to the orignal directory
                os.chdir(current_dirP_str)


class IrisWrapperExecute:
    """Creator of the SSEC-iris bash wrapper execute script."""

    template_fileN_str = 'execute_wrapper_iris.jinja'

    def __init__(self,
                 conda_pckg_manifest_obj: PackageManifest = None,
                 pckg_manifest_obj_lst: List[PackageManifest] = None,
                 env_dct: Dict[str, str] = None,
                 yaml_cfg_fileP_str: str = None):
        """

        Parameter
        ---------
        conda_pckg_manifest_obj: PackageManifest
            The conda package manifest.
        pckg_manifest_obj_lst: list of PackageManifest
            A list of package manifests.
        env_dct: dict of str
            Environmental variables that has to be set in the wrapper script.
        yaml_cfg_fileP_str: str
            The yaml config file that has the information regarding `conda_pckg_manifest_obj`,
            `pckg_manifest_obj_lst` and `env_dct` if none of these have been provided."""

        if (conda_pckg_manifest_obj is None) and (pckg_manifest_obj_lst is None) and (env_dct is None):
            if yaml_cfg_fileP_str is None:
                err_str = 'The YAML configuration file has to be provided if no other ' \
                    + 'parameters are provided.'
                raise ValueError(err_str)

            elif os.path.exists(yaml_cfg_fileP_str) is False:
                err_str = f'Could not find the file "{yaml_cfg_fileP_str}"'
                raise FileNotFoundError(err_str)

            conda_pckg_manifest_obj, pckg_manifest_obj_lst, env_dct = \
                self._read_yaml_cfg(yaml_cfg_fileP_str)

        self.conda_pckg_manifest_obj = conda_pckg_manifest_obj
        self.pckg_manifest_obj_lst = pckg_manifest_obj_lst
        self.env_dct = env_dct

        # Create to file path to the jinja file
        dirP_str = os.path.join(get_dirP_of__file__(__file__), 'templates')
        self.template_fileP_str = os.path.join(dirP_str, self.template_fileN_str)
        if os.path.exists(self.template_fileP_str) is False:
            err_str = f'The Jinja2 template file "{self.template_fileP_str}" does not exists.'
            raise FileNotFoundError(err_str)

    @staticmethod
    def _read_yaml_cfg(yaml_cfg_fileP_str) -> Tuple[PackageManifest, List[PackageManifest], dict]:
        # Read the YAML config file
        with open(yaml_cfg_fileP_str, 'r') as file_obj:
            yaml_cfg_dct = yaml.load(stream=file_obj)

        # Create the conda package manifest
        conda_pkcg_manifest_obj = PackageManifest(**yaml_cfg_dct['CondaPackageManifest'])

        # Create the list of package manifests
        pkcg_manifest_obj_lst = []
        for _, manifest_cfg_dct in yaml_cfg_dct['PackageManifest'].items():
            pkcg_manifest_obj_lst.append(PackageManifest(**manifest_cfg_dct))

        # Get the environment dictionary
        env_dct = yaml_cfg_dct['IrisWrapperExecute']['env_dct']

        return conda_pkcg_manifest_obj, pkcg_manifest_obj_lst, env_dct

    def create(self, dst_fileP_str: str = None) -> Union[str, None]:
        """Create the execute wrapper script, to be executed on iris.

        Parameters
        ----------
        dst_fileP_str: str
            The file path of where the wrapper script is to be written to; optional.

        Returns
        -------
        str:
            If dst_fileP_str is None, then the execute wrapper script string
            is returned."""

        # Create the named tuple enviroment list
        env_obj_cls = namedtuple('env_obj', ['name', 'value'])
        env_obj_lst = []
        for key_str, item_str in self.env_dct.items():
            env_obj_lst.append(env_obj_cls(name=key_str, value=item_str))

        # Read in the Jinja template for the bash file
        with open(self.template_fileP_str, 'r') as file_obj:
            template_bash_obj = jinja2.Template(file_obj.read(), trim_blocks=True)

        # Create the keyword directory for the jinja template
        kwargs_dct = {
            'conda_pckg_manifest_obj': self.conda_pckg_manifest_obj,
            'pckg_manifest_obj_lst': self.pckg_manifest_obj_lst,
            'env_obj_lst': env_obj_lst
        }

        # Write out the bash wrapper script
        if dst_fileP_str is not None:
            with open(dst_fileP_str, 'w') as file_obj:
                file_obj.write(template_bash_obj.render(**kwargs_dct))
        else:
            return template_bash_obj.render(**kwargs_dct)
