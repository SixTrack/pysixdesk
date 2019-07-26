# pySixDesk

<img src="CERN-logo.png" align="right">

pySixDesk is a python interface for SixTrack jobs for large parameter scans.
It allows to set up and manage job submission, gather results and analyse them.

pySixDesk comes as a python library; hence, it can be imported into a python terminal or into custom-made python scripts.
The library also comes with python wrapper scripts, such that specific commands can be issued directly from the login terminal.

The interface requires python3, and it is being developed with `python3.6.8`.

## Authors

X.&nbsp;Lu (CSNS, CERN),
A.&nbsp;Mereghetti (CERN).

## Environment

The native environment of pySixDesk is CERN's `lxplus` login service; the guidelines in the following will assume that this is the case.
pySixDesk embeds built-in commands for:
   * `AFS` and `openAFS` for disk storage;
   * `kerberos`, for login and user identification;
   * `htcondor`, as batch service native at CERN;
   * `BOINC`, as additional batch service for long-term, large simulation campaigns;
   * `sqlite`, for the database monitoring the progress of jobs and storing data;
   * `python3`, as main language.
   
In case of using pySixDesk on a local machine, please make sure that all these packages are available;

## How to use

pySixDesk is a library of utilities and it does not provide a user workspace.
Hence, please keep separated the library from your own workspace.

### How to download
pySixDesk is still under development. Hence, the best is to download it as a git package:
   1. go to https://github.com/SixTrack/pysixdesk and fork the project;
   1. clone it to your local file system:
   ```shell
git clone https://github.com/SixTrack/pysixdesk.git
```

In case you need to perform some development, it is common practice with git repositories to track your changes under a branch in your git fork and, once you are happy with the changes, make a request to apply them also upstream, such that every other user can profit from your contribution:
   1. add the `upstream` main project: `git remote add upstream git@github.com:SixTrack/pysixdesk.git`
   1. develop on a new branch: `git checkout -b <myBranch>`
   1. when the development is over, create a pull request via the web-interface of `github.com`.

### Shell set-up
It is recommended to use pySixDesk from the python shell.
Please remember to use python3.
On `lxplus`, python3 is available as `python3` command, since the default `python` command uses version `2.7.5`.

In order to use the library, it is essential to declare in your live python environment the path where the `pysixdesk` package can be found.
This can be accomplished adding the path to the `pysixdesk` package to the `PYTHONPATH` environment variable (in the following, `$pysixdesk_path` is the full path to pysixdesk), eg:
```shell
export PYTHONPATH=$PYTHONPATH:$pysixdesk_path/
```
or to add it to the `sys.path` list, eg:
```python
import sys
sys.path.append(<path_to_pysixdesk>/)
```
The former option can be made permanent for every python terminal in any linux terminal copying the above definition into the ```.bashrc``` file.
The only drawback to this approach is that every terminal will be affected by this setting.
It is probably more convenient to create an alias like the following one in you ```.bashrc``` file:
```shell
alias loadPySixDesk="export PYTHONPATH=$PYTHONPATH:$pysixdesk_path/"
```

## Simple use
This short guide will explain how to set up a quick toy study.
By default the jobs will be submitted to HTCondor. If you want to use a different management system, you need to create a new cluster class with the interface (Cluster) defined in the `submission.py` module and specifiy it in the `config.py` script.

   1. prepare the workspace. To do so, you have to create an instance of the parent class `StudyFactory`, which handles the workspace. If no argument is given, the default location `./sandbox` is used:
   
      ```python
      from pysixdesk import Study
      from pysixdesk import WorkSpace
      myWS = WorkSpace('./myWS')
      ```
   
   1. prepare necessary folders (e.g. `./myTest/studies/test`) and copy template files (including `config.py`) for a study. If not argument is given, the default study name is `test` (if no studies are present) or `study_???` (with `???` being a zero-padded index of the study, calculated from the existing ones):
   
      ```python
      myWS.init_study('myStudy')
      ```
   
   1. edit the `config.py` file to add scan parameters;
   
   1. load definition of study in `config.py` and create/update database:
   
      ```python
      myStudy = myWS.load_study('myStudy')
      myStudy.update_db() # only needed for a new study or when parameters are changed
      ```

   1. prepare and submit MADX jobs and sixtrack one turn jobs, and collect results:
   
      ```python
      myStudy.prepare_preprocess_input()
      myStudy.submit(0, 5) # 0 stands for preprocess job, 5 is trial number 
      myStudy.collect_result(0) # collect results locally
      ```

   1. prepare and submit actual sixtrack jobs, and collect results:

      ```python
      myStudy.prepare_sixtrack_input()
      or
      myStudy.prepare_sixtrack_input(True) #True: submit jobs to Boinc
      myStudy.submit(1, 5) # 1 stands for sixtrack job, 5 is trial number 
      myStudy.collect_result(1) # 1 stands for sixtrack job 
      or
      myStudy.collect_result(1, True) # True: collect results from boinc spool directory
      ```

In order to use a custom cluster, make sure the file containing your custom cluster class in your PYTHONPATH and simply change the cluster_class attribute in `config.py` to point to the desired class:

`config.py:`

  ```python
   import cluster
   ...
   def __init__(self, name='study', location=os.getcwd()):
       super(MyStudy, self).__init__(name, location)
       self.cluster_class = cluster.custom
       ...
   ```


## Description for the database tables

There are ten tables in the database:

| Table Name | Description |
|:------------:|:-----------|
|**boinc\_vars**| store the config parameters for boinc jobs |
|**env** | store some general parameters for the jobs, e.g.: madx\_exe, sixtrack\_exe, study\_path |
|**templates** | store the template files, e.g. mask file of madx job, fort.3 mother file |
|**preprocess\_wu**| store the machine parameters for scanning and some general information for preprocess job|
|**preprocess\_task**| store general information for the preprocess task, every submission will be recorded|
|**oneturn\_sixtrack\_wu**| store the input parameters of oneturn sixtrack job|
|**oneturn\_sixtrack\_result**| store the result of oneturn sixtrack job|
|**sixtrack\_wu**| store the sixtrack parameters for scanning and general information for sixtrack job|
|**sixtrack\_task**| store general information for the sixtrack task, also record every submission|
|**six\_results**| store the results of sixtrack jobs, for the moment keep same column name with old sixdb|

The detailed structure of these tables, please view the doc [Table.md](./doc/Table.md).
