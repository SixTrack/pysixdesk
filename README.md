# pySixDesk

<img src="CERN-logo.png" align="right">

pySixDesk is a python interface to manage and control the work flow of SixTrack jobs.
It comes as a python library that can be imported into a python terminal.
In addition, python wrapping scripts are at user disposal, such that specific commands can be issued directly from the login terminal.
The interface is meant to ease management of studies involving large parameter scans; the interface covers input generation, job submission and management, analysis.

The interface requires python3 - tool developed with `python3.6.3`.

## Authors

X.&nbsp;Lu (CSNS, CERN),
A.&nbsp;Mereghetti (CERN).

## Environment

The native environment of pySixDesk is CERN's `lxplus` login service. Hence, pySixDesk embeds built-in commands for:
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

pySixDesk is still under development. Hence, the best is to download it as a git package:
   1. got to https://github.com/SixTrack/pysixdesk and fork the project;
   1. clone it to your local file system:
   ```shell
git clone https://github.com/SixTrack/pysixdesk.git
```

In case you need to perform some development, it is common practice with git repositories to:
   1. add the `upstream` main project: ```git remote add upstream git@github.com:SixTrack/pysixdesk.git```
   1. develop on a new branch: ```git checkout -b <myBranch>```
   1. when the development is over, create a pull request via the web-interface of `github.com`.

### Shell set-up
Add the pysixdesk to the python path. Edit the ```.bashrc``` script and add the following command in the end:
```shell
export PYTHONPATH=$PYTHONPATH:$pysixdesk_path/lib
```
```$pysixdesk_path``` is the full path to pysixdesk.

If you want to use pysixdesk from python teminal, and you need to add the pysixdesk path to the system path:
```shell
import sys
sys.path.append(<path to pysixdesk/lib>)
```

## Simple use

Then you can create a '''StudyFactory''' instance to initialize a workspace for studies
```shell
from study import Study, StudyFactory
a = StudyFactory(location)
```
where ```location``` is the path of the workspace, the default value is
```sandbox```. Before creating a new study,
you should prepare the study directory with the following function:
```shell
a.prepare_study('test')
```
This function will create a directory named ```test``` in the subfolder
```studies ``` of the workspace and copy the required files and ```config.py```
file.
You can edit the ```config.py``` file to add scan parameters. 
Then you can test the program with the following codes: 

```shell
test = a.new_study('test')
test.update_db()#only need for a new study or when parameters are changed
test.prepare_preprocess_input()
test.submit(0, 5)#0 stand for preprocess job, 5 is trial number 
```
By default the jobs will be submitted to HTCondor. If you want to use a different
management system, you need to create a new cluster class with the interface (Cluster)
defined in the ```submission.py``` module and specifiy it in the config.py script.

After the jobs are finished, you can call the collect function to collect the
results and store into database:
```shell
test.collect_result(0, 5)
```
And you can specify the 'platform' argument to submit a
collection job to HTCondor:
```shell
test.collect_result(0, 5, 'htcondor')
```
For sixtrack jobs there are same functions:
```shell
test.prepare_sixtrack_input()
test.submit(1, 5)
test.collect_result(1, 5)
```
