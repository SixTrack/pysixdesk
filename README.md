# pySixDesk

<img src="CERN-logo.png" align="right">

pySixDesk is a python interface to manage and control the work flow of SixTrack jobs.

## Authors

X.&nbsp;Lu (CSNS, CERN),
A.&nbsp;Mereghetti (CERN).

##Simple usage

Clone pysixdesk to your home path in lxplus.

cd pysixdesk/templates/

change the souce_path to the path where fort.3.mother* and *.mask files located

change the dest_path to wherever you want.

Then go to the testspace/run directory and run the command:

./mad6t_oneturn.py ../../templates/mad6t.ini

#usage for study.py

In python shell:

import study

study1 = study.Study()

study1.from_env_file('scan_definitions', 'sixdeskenv', 'sysenv')

study1.prepare_mad6t_input('./templates/mad6t.ini', dest)

You will get the input config file for madx and one turn sixtrack job
