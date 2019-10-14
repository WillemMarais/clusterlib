import os
import shutil
import logging
import getpass
import tempfile
from typing import List, Union

log_obj = logging.getLogger(__name__)


def get_scratch_dirP(add_pid_bl = True):
    """Print the location of the scatch directory. If the environment variable SCRATCHDIR exists, it will be used
    as the location of the scratch directory. Otherwise, the scatch directory '/scratch/$USER' will be used. If the
    scratch directory does not exists, it will be created.

    Parameters
    ----------
    add_pid_bl: bool
        Add the process PID value to the directory path.

    Returns
    -------
    str:
        The directory path string."""

    if os.path.exists('/scratch') is True:
        default_scratch_dirP_str = os.path.join('/', 'scratch', getpass.getuser())
    else:
        default_scratch_dirP_str = os.path.join(os.environ['HOME'], 'scratch')

    dirP_str = os.getenv('SCRATCHDIR', default_scratch_dirP_str)

    if add_pid_bl is True:
        dirP_str = os.path.join(dirP_str, str(os.getpid()))

    os.makedirs(dirP_str, exist_ok = True)

    return dirP_str


class TFile:
    """TFile stands for TranscendedFile. The idea is the while the file is being read or written to, the operations are
    done a local file system. Once the file has been closed, the file is moved to its actual destination.

    The intended use of this class is when files are be worked on that are on a Lustre filesystem which is good for
    large volume data transfer, but not for small volume data transfer."""

    def __init__(self, fileP_str: str, tmp_dirP_str: str = None, base_dirP_str: str = None, overwrite_bl: bool = True,
                 do_nothing_bl: bool = False):
        """

        Parameters
        ----------
        fileP_str: str
            The path to where the file will be written to.
        tmp_dirP_str: str
            The temporary location where to keep the file. This parameter is optional.
        base_dirP_str: str
            If this parameter is set, then the temporary file path will be equal to
            fileP_str.replace(base_dirP_str, tmp_dirP_str). The purpose of this parameter is to make it easier to
            find the temporary file when debugging the software.
        overwrite_bl: bool
            TODO
        do_nothing_bl: bool
            If True, no files are copied or deleted.
        """

        self.transcended_fileP_str = fileP_str
        if (tmp_dirP_str is None) or (isinstance(tmp_dirP_str, str) is False):
            self.temp_dirP_obj = tempfile.TemporaryDirectory()
            self.temp_dirP_str = str(self.temp_dirP_obj)
        else:
            self.temp_dirP_obj = None
            self.temp_dirP_str = tmp_dirP_str

        if (base_dirP_str is None) or (isinstance(base_dirP_str, str) is False) or (base_dirP_str == ''):
            self.local_fileP_str = os.path.join(self.temp_dirP_str, os.path.basename(fileP_str))
        else:
            self.local_fileP_str = fileP_str.replace(base_dirP_str, self.temp_dirP_str)

        self.overwrite_bl = overwrite_bl
        self.do_nothing_bl = do_nothing_bl

    def copyfrom(self):
        """Copy the file from the remote file system."""

        if self.do_nothing_bl is False:
            if os.path.exists(self.transcended_fileP_str) is True:
                os.makedirs(os.path.dirname(self.local_fileP_str), exist_ok = True)

                if os.path.isdir(self.transcended_fileP_str) is True:
                    log_obj.debug('Copying from directory "{:s}" to "{:s}"'.format(self.transcended_fileP_str,
                                                                                   self.local_fileP_str))
                    shutil.copytree(self.transcended_fileP_str, self.local_fileP_str)
                else:
                    log_obj.debug('Copying from file "{:s}" to "{:s}"'.format(self.transcended_fileP_str,
                                                                              self.local_fileP_str))
                    shutil.copy(self.transcended_fileP_str, self.local_fileP_str)
            else:
                log_obj.error('The file "{:s}" does not exists'.format(self.transcended_fileP_str))

    def copyto(self):
        """Copy the file to the remote file system."""

        if self.do_nothing_bl is False:
            if os.path.exists(self.transcended_fileP_str) is True and self.overwrite_bl is False:
                log_str = 'The file / direction "{:s}" already exists and the will not be overwritten'
                log_obj.debug(log_str.format(self.transcended_fileP_str))

                return None

            os.makedirs(os.path.dirname(self.transcended_fileP_str), exist_ok = True)

            if os.path.exists(self.local_fileP_str) is True:
                if os.path.isdir(self.local_fileP_str) is True:
                    if os.path.exists(self.transcended_fileP_str) is True and self.overwrite_bl is True:
                        log_obj.debug('Removing existing directory "{:s}"'.format(self.transcended_fileP_str))
                        shutil.rmtree(self.transcended_fileP_str)

                    log_obj.debug('Copying to directory "{:s}" from "{:s}"'.format(self.local_fileP_str,
                                                                                   self.transcended_fileP_str))
                    shutil.copytree(self.local_fileP_str, self.transcended_fileP_str)
                else:
                    if os.path.exists(self.transcended_fileP_str) is True and self.overwrite_bl is True:
                        log_obj.debug('Removing existing file "{:s}"'.format(self.transcended_fileP_str))
                        os.remove(self.transcended_fileP_str)

                    log_obj.debug('Copying to file "{:s}" from "{:s}"'.format(self.local_fileP_str,
                                                                              self.transcended_fileP_str))
                    shutil.copy(self.local_fileP_str, self.transcended_fileP_str)
            else:
                log_obj.error('The file "{:s}" does not exists'.format(self.local_fileP_str))

    def cleanup(self):
        """Remove the temporary directory."""

        if self.do_nothing_bl is False:
            if os.path.exists(self.local_fileP_str) is True:
                if self.temp_dirP_obj is not None:
                    self.temp_dirP_obj.cleanup()

                elif os.path.isdir(self.local_fileP_str) is True:
                    log_obj.debug('Removing directory "{:s}"'.format(self.local_fileP_str))
                    shutil.rmtree(self.local_fileP_str)

                else:
                    log_obj.debug('Removing file "{:s}"'.format(self.local_fileP_str))
                    os.remove(self.local_fileP_str)


class TFileFrom(TFile):
    def __init__(self, fileP_str: str, return_file_bl: bool = False, tmp_dirP_str: str = None,
                 base_dirP_str: str = None, do_nothing_bl: bool = False):
        """

        Parameters
        ----------
        fileP_str: str
            The path to where the file will be copied from.
        return_file_bl: bool
            If True, whatever file / directory was copied from the remote system is copied back, assuming that
            there was some local modification.
        tmp_dirP_str: str
            The temporary location where to keep the file. This parameter is optional.
        base_dirP_str: str
            If this parameter is set, then the temporary file path will be equal to
            fileP_str.replace(base_dirP_str, tmp_dirP_str). The purpose of this parameter is to make it easier to
            find the temporary file when debugging the software.
        do_nothing_bl: bool
            If True, no files are copied or deleted.
        """

        super(TFileFrom, self).__init__(fileP_str, tmp_dirP_str, base_dirP_str, do_nothing_bl = do_nothing_bl)
        self.return_file_bl = return_file_bl

    def _enter(self):
        self.copyfrom()
        return self

    def __enter__(self):
        return self._enter()

    def _exit(self):
        if self.return_file_bl is True:
            self.copyto()
        self.cleanup()

    def __exit__(self, exception_type_obj, exception_value_obj, traceback_obj):
        return self._exit()


class TFileTo(TFile):
    def __init__(self, fileP_str: str, tmp_dirP_str: str = None, base_dirP_str: str = None, do_nothing_bl: bool = False):
        """

        Parameters
        ----------
        fileP_str: str
            The path to where the file will be written to.
        tmp_dirP_str: str
            The temporary location where to keep the file. This parameter is optional.
        base_dirP_str: str
            If this parameter is set, then the temporary file path will be equal to
            fileP_str.replace(base_dirP_str, tmp_dirP_str). The purpose of this parameter is to make it easier to
            find the temporary file when debugging the software.
        do_nothing_bl: bool
            If True, no files are copied or deleted.
        """

        super(TFileTo, self).__init__(fileP_str, tmp_dirP_str, base_dirP_str, do_nothing_bl = do_nothing_bl)

    def _enter(self):
        return self

    def __enter__(self):
        return self._enter()

    def _exit(self):
        self.copyto()
        self.cleanup()

    def __exit__(self, exception_type_obj, exception_value_obj, traceback_obj):
        return self._exit()


class TFileToOrFrom(TFile):
    """If the file file that is too be created already exists, then this object behaves like TFileFrom, otherwise it
    behaves likes TFileTo."""

    def __init__(self, fileP_str: str, return_file_bl: bool = False, tmp_dirP_str: str = None,
                 base_dirP_str: str = None, do_nothing_bl: bool = False):
        """

        Parameters
        ----------
        fileP_str: str
            The path to where the file will be either written to or read from.
        return_file_bl: bool
            If True, whatever file / directory was copied from the remote system is copied back, assuming that
            there was some local modification.
        tmp_dirP_str: str
            The temporary location where to keep the file. This parameter is optional.
        base_dirP_str: str
            If this parameter is set, then the temporary file path will be equal to
            fileP_str.replace(base_dirP_str, tmp_dirP_str). The purpose of this parameter is to make it easier to
            find the temporary file when debugging the software.
        do_nothing_bl: bool
            If True, no files are copied or deleted.
        """

        super(TFileToOrFrom, self).__init__(fileP_str, tmp_dirP_str, base_dirP_str, do_nothing_bl = do_nothing_bl)
        self.return_file_bl = return_file_bl

        if os.path.exists(fileP_str) is True:
            self.behave_like_TFileTo_bl = False  # Behave like TFileFrom
        else:
            self.behave_like_TFileTo_bl = True  # Behave like TFileTo

    def __enter__(self):
        if self.behave_like_TFileTo_bl is True:
            return self
        else:
            self.copyfrom()
            return self

    def __exit__(self, exception_type_obj, exception_value_obj, traceback_obj):
        if self.behave_like_TFileTo_bl is True:
            self.copyto()
            self.cleanup()
        else:
            if self.return_file_bl is True:
                self.copyto()
            self.cleanup()


class TFileCollection:
    def __init__(self, tfile_obj_lst: List[Union[TFileFrom, TFileTo]]):
        self.tfile_obj_lst = tfile_obj_lst

    def __enter__(self):
        for tfile_obj in self.tfile_obj_lst:
            tfile_obj._enter()

        return self

    def __exit__(self, exception_type_obj, exception_value_obj, traceback_obj):
        for tfile_obj in self.tfile_obj_lst:
            tfile_obj._exit()
