# The structure of database table

## boinc_vars
| Field                   | Type    | Description
|-------------------------|---------|-----------
| workunitName            | text    | 
| fpopsEstimate           | double  | 
| fpopsBound              | double  | 
| memBound                | int(11) | 
| diskBound               | int(11) | 
| delayBound              | int(11) | 
| redundancy              | int(11) | 
| copies                  | int(11) | 
| errors                  | int(11) | 
| numIssues               | int(11) | 
| resultsWithoutConcensus | int(11) | 
| appName                 | text    | 
| appVer                  | int(11) | 

## env
| Field          | Type    | Description
|----------------|---------|------------
| madx_exe       | text    | 
| sixtrack_exe   | text    | 
| study_path     | text    | 
| preprocess_in  | text    | 
| preprocess_out | text    | 
| sixtrack_in    | text    | 
| sixtrack_out   | text    | 
| gather         | text    | 
| templates      | text    | 
| boinc_spool    | text    | 
| test_turn      | int(11) | 
| emit           | double  | 
| gamma          | double  | 
| kmax           | int(11) | 
| boinc_work     | text    | 
| boinc_results  | text    | 
| surv_percent   | int(11) | 

## templates
| Field                   | Type    | Description 
|-------------------------|---------|------
| workunitName            | text    |   
| fpopsEstimate           | double  |   
| fpopsBound              | double  |   
| memBound                | int(11) |   
| diskBound               | int(11) |   
| delayBound              | int(11) |   
| redundancy              | int(11) |   
| copies                  | int(11) |   
| errors                  | int(11) |   
| numIssues               | int(11) |   
| resultsWithoutConcensus | int(11) |   
| appName                 | text    |   
| appVer                  | int(11) |   

## preprocess_wu
| Field      | Type       | Description 
|------------|------------|------
| wu_id      | int(11)    |   
| job_name   | text       |   
| input_file | blob       |   
| batch_name | text       |   
| unique_id  | text       |   
| status     | text       |   
| task_id    | int(11)    |   
| mtime      | bigint(20) |   
| SEEDRAN    | int(11)    |   
| QP         | int(11)    |   
| IOCT       | int(11)    |   

## preprocess_task
| Field       | Type       | Description 
|-------------|------------|------
| task_id     | int(11)    |   
| wu_id       | int(11)    |   
| madx_in     | blob       |   
| madx_stdout | blob       |   
| job_stdout  | blob       |   
| job_stderr  | blob       |   
| job_stdlog  | blob       |   
| status      | text       |   
| mtime       | bigint(20) |   
| fort_2      | mediumblob |   
| fort_3_mad  | mediumblob |   
| fort_3_aux  | mediumblob |   
| fort_8      | mediumblob |   
| fort_16     | mediumblob |   
| fort_34     | mediumblob |   

## oneturn_sixtrack_wu
| Field        | Type    | Description 
|--------------|---------|------
| turnss       | int(11) | 
| nss          | int(11) | 
| ax0s         | double  | 
| ax1s         | double  | 
| imc          | int(11) | 
| iclo6        | int(11) | 
| writebins    | int(11) | 
| ratios       | int(11) | 
| Runnam       | text    | 
| idfor        | int(11) | 
| ibtype       | int(11) | 
| ition        | int(11) | 
| CHRO         | text    | 
| TUNE         | text    | 
| POST         | text    | 
| POS1         | text    | 
| ndafi        | int(11) | 
| tunex        | double  | 
| tuney        | double  | 
| inttunex     | double  | 
| inttuney     | double  | 
| DIFF         | text    | 
| DIF1         | text    | 
| pmass        | double  | 
| emit_beam    | double  | 
| e0           | int(11) | 
| bunch_charge | double  | 
| CHROM        | int(11) | 
| chrom_eps    | double  | 
| dp1          | double  | 
| dp2          | double  | 
| chromx       | int(11) | 
| chromy       | int(11) | 
| rfvol        | double  | 
| sigz         | double  | 
| sige         | double  | 

## oneturn_sixtrack_result
| Field     | Type       | Description 
|-----------|------------|------
| task_id   | int(11)    |  
| wu_id     | int(11)    |  
| betax     | float      |  
| betax2    | float      |  
| betay     | float      |  
| betay2    | float      |  
| tunex     | float      |  
| tuney     | float      |  
| chromx    | float      |  
| chromy    | float      |  
| x         | float      |  
| xp        | float      |  
| y         | float      |  
| yp        | float      |  
| z         | float      |  
| zp        | float      |  
| chromx_s  | float      |  
| chromy_s  | float      |  
| chrom_eps | float      |  
| tunex1    | float      |  
| tuney1    | float      |  
| tunex2    | float      |  
| tuney2    | float      |  
| mtime     | bigint(20) |  

## sixtrack_wu
| Field         | Type       | Description 
|---------------|------------|------
| wu_id         | int(11)    | 
| preprocess_id | int(11)    | 
| job_name      | text       | 
| input_file    | blob       | 
| batch_name    | text       | 
| unique_id     | text       | 
| status        | text       | 
| task_id       | int(11)    | 
| boinc         | text       | 
| mtime         | bigint(20) | 
| turnss        | int(11)    | 
| nss           | int(11)    | 
| ax0s          | double     | 
| ax1s          | double     | 
| imc           | int(11)    | 
| iclo6         | int(11)    | 
| writebins     | int(11)    | 
| ratios        | int(11)    | 
| Runnam        | text       | 
| idfor         | int(11)    | 
| ibtype        | int(11)    | 
| ition         | int(11)    | 
| CHRO          | text       | 
| TUNE          | text       | 
| POST          | text       | 
| POS1          | text       | 
| ndafi         | int(11)    | 
| tunex         | double     | 
| tuney         | double     | 
| inttunex      | double     | 
| inttuney      | double     | 
| DIFF          | text       | 
| DIF1          | text       | 
| pmass         | double     | 
| emit_beam     | double     | 
| e0            | int(11)    | 
| bunch_charge  | double     | 
| CHROM         | int(11)    | 
| chrom_eps     | double     | 
| dp1           | double     | 
| dp2           | double     | 
| chromx        | int(11)    | 
| chromy        | int(11)    | 
| rfvol         | double     | 
| sigz          | double     | 
| sige          | double     | 
| amp           | text       | 
| kang          | int(11)    | 

## sixtrack_task
| Field      | Type       | Description 
|------------|------------|------
| task_id    | int(11)    | 
| wu_id      | int(11)    | 
| fort3      | blob       | 
| job_stdout | blob       | 
| job_stderr | blob       | 
| job_stdlog | blob       | 
| status     | text       | 
| mtime      | bigint(20) | 
| fort_10    | blob       | 

## six_results
| Field        | Type       | Description 
|--------------|------------|------
| six_input_id | int(11)    | 
| row_num      | int(11)    | 
| turn_max     | int(11)    | 
| sflag        | int(11)    | 
| qx           | float      | 
| qy           | float      | 
| betx         | float      | 
| bety         | float      | 
| sigx1        | float      | 
| sigy1        | float      | 
| deltap       | float      | 
| dist         | float      | 
| distp        | float      | 
| qx_det       | float      | 
| qx_spread    | float      | 
| qy_det       | float      | 
| qy_spread    | float      | 
| resxfact     | float      | 
| resyfact     | float      | 
| resorder     | int(11)    | 
| smearx       | float      | 
| smeary       | float      | 
| smeart       | float      | 
| sturns1      | int(11)    | 
| sturns2      | int(11)    | 
| sseed        | float      | 
| qs           | float      | 
| sigx2        | float      | 
| sigy2        | float      | 
| sigxmin      | float      | 
| sigxavg      | float      | 
| sigxmax      | float      | 
| sigymin      | float      | 
| sigyavg      | float      | 
| sigymax      | float      | 
| sigxminld    | float      | 
| sigxavgld    | float      | 
| sigxmaxld    | float      | 
| sigyminld    | float      | 
| sigyavgld    | float      | 
| sigymaxld    | float      | 
| sigxminnld   | float      | 
| sigxavgnld   | float      | 
| sigxmaxnld   | float      | 
| sigyminnld   | float      | 
| sigyavgnld   | float      | 
| sigymaxnld   | float      | 
| emitx        | float      | 
| emity        | float      | 
| betx2        | float      | 
| bety2        | float      | 
| qpx          | float      | 
| qpy          | float      | 
| version      | float      | 
| cx           | float      | 
| cy           | float      | 
| csigma       | float      | 
| xp           | float      | 
| yp           | float      | 
| delta        | float      | 
| dnms         | float      | 
| trttime      | float      | 
| mtime        | bigint(20) | 
