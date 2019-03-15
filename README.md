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

Then you can add scan parameters to the ```lib/config.py``` script.
You can test the program with the following codes: 

```shell
from study import Study, StudyFactory
a = StudyFactory()
test = a.new_study('test')
test.structure()
test.prepare_madx_single_input()
test.submit_mad6t(place='./run')
```
```place='./run``` is to set the place to run the jobs.
You will find the result in ```sandbox``` directory

Note: Please don't do operations in the pysixdesk folder!
