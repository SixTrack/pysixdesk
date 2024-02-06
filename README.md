# pySixDesk

<img src="CERN-logo.png" align="right">

pySixDesk is a python interface to managing SixTrack jobs for large parameter scans.
It allows to set up and manage job submission, gather results and analyse them.

pySixDesk comes as a python library; hence, it can be imported into a python terminal or into custom-made python scripts.
The library also comes with python wrapper scripts, such that specific commands can be issued directly from the login terminal.

The interface requires python3, and it is being developed with `python3.6.8`.

## Authors

L.&nbsp;Coyle (CERN),
X.&nbsp;Lu (CSNS, CERN),
A.&nbsp;Mereghetti (CERN).

## Environment

The native environment of pySixDesk is CERN's `lxplus` login service; the guidelines in the following will assume that this is the case.
pySixDesk embeds built-in commands for:
   * `AFS` and `openAFS` for disk storage;
   * `kerberos`, for login and user identification;
   * `htcondor`, as batch service native at CERN;
   * `BOINC`, as additional batch service for long-term, large simulation campaigns;
   * `sqlite3` and `mySQL`, for the database monitoring the progress of jobs and storing data;
   * `python3`, as main language.
   
In case of using pySixDesk on a local machine, please make sure that the necessary packages are available;

## How to use

pySixDesk is a library of utilities and it does not provide a user workspace.
Hence, please keep separated the library from your own workspace.

### How to download
pySixDesk is still under development. Hence, the best is to download it as a git package:
   1. go to https://github.com/SixTrack/pysixdesk and fork the project. If you do not have a user account on github.com, please create one. You may also want to properly configure your ssh keys for managing possible pull requests;
   1. clone your fork to your local file system (`<myAccount>` is the login name of your account):
   ```shell
git clone https://github.com/<myAccount>/pysixdesk.git
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
By default the jobs will be submitted to HTCondor. If you want to use a different batch system, you need to create a new cluster class with the interface (Cluster) defined in the `submission.py` module and specifiy it in the `config.py` script.

   1. prepare the workspace. To do so, you have to create an instance of the parent class `StudyFactory`, which handles the workspace. If no argument is given, the default location `./sandbox` is used:
   
      ```python
      from pysixdesk import Study
      from pysixdesk import WorkSpace
      myWS = WorkSpace('./myWS')
      ```
   
   1. prepare necessary folders (e.g. `./myTest/studies/test`) and copy template files (including `config.py`) for a study. If no argument is given, the default study name is `test` (if no studies are present) or `study_???` (with `???` being a zero-padded index of the study, calculated from the existing ones):
   
      ```python
      myWS.init_study('myStudy')
      ```

   1. edit the `config.py` file to define the parameters you want to scan for your study as well as their range. This also includes adapting the `.mask` file and parametrizing the variables that are to be scanned (use `%` sign to declare these parameters followed by the parameter name defined in `config.py`, e.g. `%SEEDRAN`). Note that PySixDesk will set up a scan making all the possible combinations between defined parameter values (Cartesian product);
   
   1. load definition of study in `config.py` and create/update database:
   
      ```python
      myStudy = myWS.load_study('myStudy')
      myStudy.update_db() # only needed for a new study or when parameters are changed
      ```

   This initializes a database file. Depending on the type of database you choose in the `config.py` file, it is either an SQLite database stored locally, or, alternatively, a MySQL database hosted by the CERN DBonDemand Service.

   1. prepare and submit pre-processing jobs (i.e. MADX job and sixtrack one turn jobs), and collect results:
   
      ```python
      myStudy.prepare_preprocess_input()
      myStudy.submit(0, 5) # 0 stands for preprocess job, 5 is trial number 
      myStudy.collect_result(0) # collect results locally once jobs are finished
      ```

   1. prepare and submit actual sixtrack jobs, and collect results:

      ```python
      myStudy.prepare_sixtrack_input()
      or
      myStudy.prepare_sixtrack_input(True) #True: submit jobs to Boinc

      myStudy.submit(1, 5) # 1 stands for sixtrack job, 5 is trial number 

      myStudy.collect_result(1) # 1 stands for sixtrack job; collect results once finished
      or
      myStudy.collect_result(1, True) # True: collect results from boinc spool directory
      ```

In order to use a custom cluster, make sure the file containing your custom cluster class is located in your `$PYTHONPATH` and simply change the `cluster_class` attribute in `config.py` to point to the desired class:

`config.py:`

  ```python
   import cluster
   ...
   def __init__(self, name='study', location=os.getcwd()):
       super(MyStudy, self).__init__(name, location)
       self.cluster_class = cluster.custom
       ...
   ```


## Summary of Database Tables

There are ten tables in the database:

| Table Name | Description |
|:------------:|:-----------|
|**boinc\_vars**| config parameters for boinc jobs |
|**env** | general parameters for the jobs, e.g.: madx\_exe, sixtrack\_exe, study\_path |
|**templates** | template files, e.g. `.mask` file, `fort.3` mother files |
|**preprocess\_wu**| parameters of pre-process (e.g. MAD-X) job (e.g. machine parameters to be scanned and general information ) |
|**preprocess\_task**| actual instance of the pre-processing job (every submission will be recorded) |
|**oneturn\_sixtrack\_wu**| input parameters of oneturn sixtrack jobs |
|**oneturn\_sixtrack\_result**| the result of oneturn sixtrack jobs |
|**sixtrack\_wu**| sixtrack parameters for scanning and general information for sixtrack job |
|**sixtrack\_task**| actual instance of the sixtrack job (every submission will be recorded) |
|**six\_results**| the results of sixtrack jobs, for the moment keep same column name with old sixdb |

The detailed structure of these tables, please view the doc [Table.md](./doc/Table.md).
