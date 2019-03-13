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
Add the pysixdesk to the python path.
```shell
cd pysixdesk/
source setupEnvironment.sh
```

Then you can add scan parameters to the ```lib/config.py``` script.
Then you can test the program with the follow codes: 

```shell
from study import Study, StudyFactory
a = StudyFactory()
test = a.new_study('test')
test.prepare_madx_single_input()
test.submit_mad6t(place='./testspace/run')
```
```place='./testspace/run``` is to set the place to run the jobs
You will find the result in ```sandbox``` directory
