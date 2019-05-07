# pySixDesk

<img src="CERN-logo.png" align="right">

pySixDesk is a python interface to manage and control the work flow of SixTrack jobs.

## Authors

X.&nbsp;Lu (CSNS, CERN),
A.&nbsp;Mereghetti (CERN).

## Simple usage

Clone pysixdesk to your home path in lxplus.
```shell
git clone https://github.com/SixTrack/pysixdesk.git
```
Add the pysixdesk to the python path. Edit the ```.bashrc``` script
and add the following command in the end:
```shell
export PYTHONPATH=$PYTHONPATH:$pysixdesk_path/lib
```
```$pysixdesk_path``` is the full path to pysixdesk.

If you want to use pysixdesk from python teminal, and you need to add the 
pysixdesk path to the system path:
```shell
import sys
sys.path.append(<path to pysixdesk/lib>)
```

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

Note: Please don't do operations in the pysixdesk folder!
