# The structure of database table

## boinc_vars
This table will have only one line

| Field                   | Type    | Description
|-------------------------|---------|-----------
| workunitName            | text    | The work unit name 
| fpopsEstimate           | double  | Getting the fpops estimate and multiplying it with 10 to get the bound 
| fpopsBound              | double  | fpopsEstimate*1000 
| memBound                | int(11) | The workunit will only be sent to clients with at least this much available RAM. If exceeded the workunit will be aborted 
| diskBound               | int(11) | An upper bound on the amount of disk space requred to process the workunit 
| delayBound              | int(11) | An upper bound on the time (in seconds) between sending a result to a client and receiving a reply 
| redundancy              | int(11) | The number of redundant calculations. Set to two or more ot achieve redundancy 
| copies                  | int(11) | The number of copies of the workunit to issue to clients
| errors                  | int(11) | The number of errors from clients before the workunit is declared to have an error 
| numIssues               | int(11) | The total number of clients to issue the work unit to before it is declared to have an error 
| resultsWithoutConcensus | int(11) | The total number of returned results without a concensus is found before the workunit is declared to have an error 
| appName                 | text    | The app name (usually is sixtrack) 
| appVer                  | int(11) | The app version (e.g. 50210)

## env
This table will have only one line

| Field          | Type    | Description
|----------------|---------|------------
| madx_exe       | text    | The madx executable
| sixtrack_exe   | text    | The sixtrack executable 
| study_path     | text    | The absolute path of this study 
| preprocess_in  | text    | The input folder for preprocess jobs 
| preprocess_out | text    | The output folder for preprocess jobs 
| sixtrack_in    | text    | The input folder for sixtrack jobs 
| sixtrack_out   | text    | The output folder for sixtrack jobs 
| gather         | text    | The input folder for gather job which is used for collecting results 
| templates      | text    | The template folder 
| boinc_spool    | text    | The boinc spool for submitting jobs to boinc 
| test_turn      | int(11) | When submitting jobs to boinc, we need to test the jobs on HTCondor firstly 
| emit           | double  | Normalized emittance 
| gamma          | double  | The relativistic  gamma 
| kmax           | int(11) | The number of phase space angles 
| boinc_work     | text    | The absolute path of work directory which is the spool directory on the boinc volume to hold the input files of sixtrack 
| boinc_results  | text    | The absolute path of results directory which is the spool directory on the bonic volume to hold the results 
| surv_percent   | int(11) | When testing jobs on HTCondor, survival percent decides whether to submit jobs to boinc

## templates
This table will have only one line

| Field     | Type | Description 
|-----------|------|------------
| mask_file | blob | The mask file of madx input file 
| fort_3    | blob | The mother file of fort.3 

## preprocess_wu
This table will have as many lines as the machine configurations

| Field      | Type       | Description 
|------------|------------|------
| wu_id      | int(11)    | The work unit id
| job_name   | text       | The job name  
| input_file | blob       | Hold all the required informations for the executable python script 'preprocess.py' which is used for runing madx job and oneturn sixtrack job
| batch_name | text       | The batch name of this job in HTCondor
| unique_id  | text       | The unique id of this job on HTCondor, usually is ClusterId.ProcId  
| status     | text       | The status of this job, 'complete', 'incomplete' or 'submitted'  
| task_id    | int(11)    | The id of the latest submitted task
| mtime      | bigint(20) | The Last modification time  
| SEEDRAN    | int(11)    | The seed for madx job   
| QP         | int(11)    | A parameter for the machine  
| IOCT       | int(11)    | A parameter for the machine  

## preprocess_task
This table have at least as many lines as the preprocess\_wu table

| Field       | Type       | Description 
|-------------|------------|------
| task_id     | int(11)    | The unique task id
| wu_id       | int(11)    | The work unit id
| madx_in     | blob       | The input file of madx job
| madx_stdout | blob       | The output file of madx job
| job_stdout  | blob       | The standard output of HTCondor job 
| job_stderr  | blob       | The standard err of HTCondor job 
| job_stdlog  | blob       | The log file of HTCondor job 
| status      | text       | The status of this task, 'Success' or 'Failed' 
| mtime       | bigint(20) | Last modification time
| fort_2      | mediumblob | fort.2
| fort_3_mad  | mediumblob | fort.3.mad
| fort_3_aux  | mediumblob | fort.3.aux
| fort_8      | mediumblob | fort.8
| fort_16     | mediumblob | fort.16
| fort_34     | mediumblob | fort.34

## oneturn_sixtrack_wu
This table will have as many lines as the machine configurations

| Field        | Type    | Description 
|--------------|---------|------
| turnss       | int(11) | The tracking turn
| nss          | int(11) | Amplitude step in normalised amplitude
| ax0s         | double  | Start amplitude in normalised unit [mm] 
| ax1s         | double  | End amplitude in normalised unit [mm] 
| imc          | int(11) | Number of variations of the relative momentum deviation 
| iclo6        | int(11) | This switch allows to calculate the 6D closed orbit and optical functions at the starting point, using the differential algebra package.
| writebins    | int(11) | This defines after how many turns data are written to output files 
| ratios       | int(11) | Denotes the emittance ratio (eII/eI) of horizontal and vertical motion 
| Runnam       | text    | The title of the job reported in fort.3 
| idfor        | int(11) | The closed orbit is added to the initial coordinates (0) or not (1)
| ibtype       | int(11) | Use the Erskine/McIntosh optimised error function of a complex number(1) or not (0) 
| ition        | int(11) | Transition energy switch 
| POST         | text    | 'POST' 
| POS1         | text    | ''
| ndafi        | int(11) | Number of data files to be processed. 
| tunex        | double  | Horizontal tune 
| tuney        | double  | Vertical tune 
| inttunex     | double  | Values of the horizontal tune (integer part) to be added to the averaged phase advance and to the Q values of the FFT analysis
| inttuney     | double  | Values of the vertical tune... 
| DIFF         | text    |'/DIFF' 
| DIF1         | text    |/ 
| pmass        | double  | Proton mass 
| emit_beam    | double  | Emittance 
| e0           | int(11) | Energy 
| bunch_charge | double  | Bunch charge 
| CHROM        | int(11) | To correct for slight differences between MAD and SixTrack the chromaticity is routinely corrected by setting 'chrom' to 1 and using chrom_eps=0.0000001 
| chrom_eps    | double  | chrome_eps 
| dp1          | double  | Momentum deviation for particle 1 
| dp2          | double  | Momentum deviation for particle 2 
| chromx       | int(11) | chromx 
| chromy       | int(11) | chromy 
| rfvol        | double  | RF Voltage 
| sigz         | double  | r.m.s bunch length 
| sige         | double  | r.m.s energy spread 

## oneturn_sixtrack_result
This table will have as many lines as the oneturn_sixtrack_wu table 

| Field     | Type       | Description 
|-----------|------------|------
| task_id   | int(11)    | The task id 
| wu_id     | int(11)    | The work unit id 
| betax     | float      | Horizontal beta-function
| betax2    | float      | Secondary horizontal beta-function 
| betay     | float      | Vertical beta-function 
| betay2    | float      | Secondary beta-function 
| tunex     | float      | Horizontal tune 
| tuney     | float      | Vertical tune 
| chromx    | float      | Horizontal chromaticity 
| chromy    | float      | Vertical chromaticity 
| x         | float      | x 
| xp        | float      | xp
| y         | float      | y 
| yp        | float      | yp 
| z         | float      | z 
| zp        | float      | zp 
| chromx_s  | float      | chromx obtained by calculation (second\_turn\_tune-first\_turn\_tune)/chrom_eps
| chromy_s  | float      | chromy obtained by calculation 
| chrom_eps | float      | Used for calculating chromaticity 
| tunex1    | float      | First turn horizontal tune 
| tuney1    | float      | First turn vertical tune 
| tunex2    | float      | Second turn horizontal tune 
| tuney2    | float      | Second turn vertical tune 
| mtime     | bigint(20) | Last modification time 

## sixtrack_wu
This table will have as many lines as the cartesian product between machine configurations and the beam phase space scan points

| Field         | Type       | Description 
|---------------|------------|------
| wu_id         | int(11)    | The work unit id 
| preprocess_id | int(11)    | The work unit id of preprocess job 
| job_name      | text       | The job name
| input_file    | blob       | Hold all the required informations for the executable python script 'sixtrack.py' which is used for running actual sixtrack job
| batch_name    | text       | The batch name of this job in HTCondor
| unique_id     | text       | The unique id of this job in HTCondor, usually is ClusterId.ProcId 
| status        | text       | The status of this job, 'complete', 'incomplete' or 'submitted'
| task_id       | int(11)    | The task id which point to a submission 
| boinc         | text       | Submit to boinc ('true') or not ('false')
| mtime         | bigint(20) | The Last modification time 
| turnss        | int(11)    | The tracking turn
| nss           | int(11)    | Amplitude step [mm]
| ax0s          | double     | Start amplitude [mm] 
| ax1s          | double     | End amplitude [mm] 
| imc           | int(11)    | Number of variations of the relative momentum deviation 
| iclo6         | int(11)    | This switch allows to calculate the 6D closed orbit and optical functions at the starting point, using the differential algebra package.
| writebins     | int(11)    | This defines after how many turns data are written to output files 
| ratios        | int(11)    | Denotes the emittance ratio (eII/eI) of horizontal and vertical motion 
| Runnam        | text       | The running name 
| idfor         | int(11)    | The closed orbit is added to the initial coordinates (0) or not (1)
| ibtype        | int(11)    | Use the Erskine/McIntosh optimised error function of a complex number(1) or not (0) 
| ition         | int(11)    | Transition energy switch 
| POST          | text       | 'POST' 
| POS1          | text       | '' 
| ndafi         | int(11)    | Number of data files to be processed. 
| tunex         | double     | Horizontal tune 
| tuney         | double     | Vertical tune 
| inttunex      | double     | Values of the horizontal tune (integer part) to be added to the averaged phase advance and to the Q values of the FFT analysis
| inttuney      | double     | Values of the vertical tune... 
| DIFF          | text       | '/DIFF' 
| DIF1          | text       | / 
| pmass         | double     | Proton mass 
| emit_beam     | double     | Emittance 
| e0            | int(11)    | Energy 
| bunch_charge  | double     | Bunch charge 
| CHROM         | int(11)    | To correct for slight differences between MAD and SixTrack the chromaticity is routinely corrected by setting 'chrom' to 1 and using chrom_eps=0.0000001 
| chrom_eps     | double     | chrome_eps 
| dp1           | double     | Momentum deviation for particle 1 
| dp2           | double     | Momentum deviation for particle 2 
| chromx        | int(11)    | chromx 
| chromy        | int(11)    | chromy 
| rfvol         | double     | RF Voltage 
| sigz          | double     | r.m.s bunch length 
| sige          | double     | r.m.s energy spread 
| amp           | text       | The amplitudes (list) 
| kang          | int(11)    | The angles in phase space (list)  

## sixtrack_task
This table at least have as many lines as the sixtrack\_wu

| Field      | Type       | Description 
|------------|------------|------
| task_id    | int(11)    | The unique task id for a submission 
| wu_id      | int(11)    | The work unit id 
| fort3      | blob       | The fort.3 file  
| job_stdout | blob       | The standard output of HTCondor job 
| job_stderr | blob       | The standard err of HTCondor job 
| job_stdlog | blob       | The log file of HTCondor job 
| status     | text       | The status of this task, 'Success' or 'Failed' 
| mtime      | bigint(20) | Last modification time
| fort_10    | blob       | The fort.10 file (result) 

## six_results
This table hold the results of sixtrack jobs which will have as many lines as the points in all scans

| Field        | Type       | Description 
|--------------|------------|------
| six_input_id | int(11)    | unique id for each sixtrack task
| row_num      | int(11)    | rownumber
| turn_max     | int(11)    | Maximum turn number
| sflag        | int(11)    | Stability Flag (0=stable 1=lost)
| qx           | float      | Horizontal Tune
| qy           | float      | Vertical Tune
| betx         | float      | Horizontal beta-function
| bety         | float      | Vertical beta-function
| sigx1        | float      | Horizontal amplitude 1st particle
| sigy1        | float      | Vertical amplitude 1st particle
| deltap       | float      | Relative momentum deviation Deltap
| dist         | float      | Final distance in phase space
| distp        | float      | Maximum slope of distance in phase space
| qx_det       | float      | Horizontal detuning
| qx_spread    | float      | Spread of horizontal detuning
| qy_det       | float      | Vertical detuning
| qy_spread    | float      | Spread of vertical detuning
| resxfact     | float      | Horizontal factor to nearest resonance
| resyfact     | float      | Vertical factor to nearest resonance
| resorder     | int(11)    | Order of nearest resonance
| smearx       | float      | Horizontal smear
| smeary       | float      | Vertical smear
| smeart       | float      | Transverse smear
| sturns1      | int(11)    | Survived turns 1st particle
| sturns2      | int(11)    | Survived turns 2nd particle
| sseed        | float      | Starting seed for random generator
| qs           | float      | Synchrotron tune
| sigx2        | float      | Horizontal amplitude 2nd particle
| sigy2        | float      | Vertical amplitude 2nd particle
| sigxmin      | float      | Minimum horizontal amplitude
| sigxavg      | float      | Mean horizontal amplitude
| sigxmax      | float      | Maximum horizontal amplitude
| sigymin      | float      | Minimum vertical amplitude
| sigyavg      | float      | Mean vertical amplitude
| sigymax      | float      | Maximum vertical amplitude
| sigxminld    | float      | Minimum horizontal amplitude (linear decoupled)
| sigxavgld    | float      | Mean horizontal amplitude (linear decoupled)
| sigxmaxld    | float      | Maximum horizontal amplitude (linear decoupled)
| sigyminld    | float      | Minimum vertical amplitude (linear decoupled)
| sigyavgld    | float      | Mean vertical amplitude (linear decoupled)
| sigymaxld    | float      | Maximum vertical amplitude (linear decoupled)
| sigxminnld   | float      | Minimumhorizontal amplitude (nonlinear decoupled)
| sigxavgnld   | float      | Mean horizontal amplitude (nonlinear decoupled)
| sigxmaxnld   | float      | Maximumhorizontal amplitude (nonlinear decoupled)
| sigyminnld   | float      | Minimum vertical amplitude (nonlinear decoupled)
| sigyavgnld   | float      | Mean vertical amplitude (nonlinear decoupled)
| sigymaxnld   | float      | Maximum vertical amplitude (nonlinear decoupled)
| emitx        | float      | Emittance Mode I
| emity        | float      | Emittance Mode II
| betx2        | float      | Secondary horizontal beta-function
| bety2        | float      | Secondary vertical beta-function
| qpx          | float      | Qx
| qpy          | float      | Qy
| version      | float      | Dummy1
| cx           | float      | Dummy2
| cy           | float      | Dummy3
| csigma       | float      | Dummy4
| xp           | float      | Dummy5
| yp           | float      | Dummy6
| delta        | float      | Dummy7
| mtime        | bigint(20) | Last modification time

# Remarks

The **preprocess job** aims to generate sixtrack input file from madx, e.g.
fort.2,fort.8,fort.16 ..., and also get some machine parameters from oneturn sixtrack job, e.g. tune, chromaticity, beta ...\
The **sixtrack job** is the actual tracking job for the study.

The **\*\_wu** tables hold the points of the scan parameters for the jobs (preprocess job or sixtrack job)\
The **\*\_task** tables record every submission of the jobs, no matter success or failure.

```shell
# Get the preprocess_wu line linked to a given task:
preprocess_task.wu_id = preprocess_wu.wu_id 
# Get the latest/reference task of a given wu:
preprocess_wu.task_id = preprocess_task.task_id 
```

**N.B.** The detailed descritption of some input parameters for sixtrack could be found in the [User's Reference Manual](https:/sixtrack.web.cern.ch/SixTrack/docs/user_full/manual.php#Ch3) of sixtrack
