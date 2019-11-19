# Clusterlib

Clusterlib is a Python package that contains tools that helps with executing Python programs on `iris`.

## Modules

### Wrapexe

Create an execution environment shell `bash` script that untars the `anaconda` execution enviroment for a `python` program that will be executed with additional custom packages. The `anaconda` execution enviroment is a pre-installed `anaconda` enviroment that has been tarred. The custom package can e.g. be `python` package that you are developing.

When multiple `slurm` jobs executes the shell `bash` script, the script uses the `flock` command to prevent the different jobs from simultaneously untaring the `anaconda` execution enviroment into the same directory. In other words, only one job untars the `anaconda` execution enviroment per execution node.

The execution environment shell `bash` script uses checksums on the tar files to check if a tar file has been updated. E.g., if the `anaconda` execution enviroment has been updated (i.e. a new package has been installed) after the completion of all the slurm jobs, the new `anaconda` execution enviroment tar file will have a different checksum compared to the previous untarred `anaconda` execution enviroment.

Follow the following steps in order to use the class `wrapexe.IrisWrapperExecute` to create the shell `bash` script.

#### Step 1 - Create miniconda package

1. On the `iris` head-node install `miniconda` in the directory `/scratch/long/<username>/miniconda` with all the necessary `python` packages.
2. tar the `miniconda` package and put it on the luster filesytem. For example,

    tar cf /scratch/long/miniconda_latest.tar /scratch/long/<username>/miniconda
    mv /scratch/long/miniconda_latest.tar /ships19/hercules/<username>/packages/iris

#### Step 2 - Create the configuration dictionary

The class `wrapexe.IrisWrapperExecute` takes a dictionary as input which describes where the `anaconda` execution enviroment package is located. The following `YAML` configuration gives an example of what should be in the dictionary.

    import yaml

    yaml_cfg_str = """---
    CondaPackageManifest:
        dst_tar_fileP_str: "/ships19/hercules/<username>/packages/iris/miniconda_latest.tar"
        dst_dirP_str: "/scratch/long/<username>/project/miniconda3"
        checksum_fileP_str: "/scratch/long/<username>/project/miniconda3_latest.tar.txt"
    PackageManifest:
        clusterlib_example:
            name_str: "clusterlib_example"
            src_dirP_str: "/home/<username>/Projects/GitLab/clusterlib"
            dst_tar_fileP_str: "/ships19/hercules/<username>/packages/iris/clusterlib_latest.tar"
            dst_dirP_str: "/scratch/long/<username>/project/clusterlib"
            checksum_fileP_str: "/scratch/long/<username>/project/clusterlib_latest.tar.txt"
    ..."""

    cfg_dct = yaml.load(stream=yaml_cfg_str, Loader=yaml.FullLoader)

The dictionary value ```dst_tar_fileP_str``` indicates where the `anaconda` execution enviroment tar file is located. The value ```dst_dirP_str``` indicates to where the  `anaconda` execution enviroment tar file should be untarred, and ```checksum_fileP_str``` indicates where the tar file's checksum should be archived.

The dictionary ```cfg_dct[PackageManifest]``` specifies the other `python` packages that should also be installed with the `anaconda` execution enviroment. The dictionary value `src_dirP_str` indicates where the `python` package is installed on the machine which will execute the class `wrapexe.IrisWrapperExecute`.

#### Step 3 - Execute `wrapexe.IrisWrapperExecute`

The following `python` code shows how to execute `wrapexe.IrisWrapperExecute`:

    # Where to write the execution environment shell script
    out_fileP_str = '/home/<username>/wrap_exe.bash'

    iris_wrap_exe_obj = IrisWrapperExecute(yaml_cfg_dct=cfg_dct)
    # Package `clusterlib` and copy it to
    # /ships19/hercules/<username>/packages/iris/clusterlib_latest.tar
    iris_wrap_exe_obj.package()
    # Create the wrapper script
    iris_wrap_exe_obj.create(out_fileP_str)

To use the wrapper script, pass the file path of the `python` script that should be executed with its arguments. For example,

    /bin/bash /home/<username>/wrap_exe.bash /home/<username>/script.py 1 2 3 1.0

### File

TODO

### Executor

TODO

### Makeflow

TODO
