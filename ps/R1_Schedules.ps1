
####Daily 
$File_Path =  "C:\Users\jeaje\Documents\JA_docs\Dev\Rule1_Investment\Proc\2.0\code\ps\R1_Daily_proc.ps1"

$T = @{
  Frequency="Daily"
  At="07:00AM"
  DaysOfWeek="Tuesday" , "Wednesday", "Thursday", "Friday", "Saturday"
  #Interval=2
}
$O = @{
  WakeToRun=$true
  StartIfNotIdle=$true
  MultipleInstancePolicy="Queue"
}
Register-ScheduledJob -Trigger $T -ScheduledJobOption $O -Name R1_Daily_Updates -FilePath $File_Path 



####Weekly 
$File_Path =  "C:\Users\jeaje\Documents\JA_docs\Dev\Rule1_Investment\Proc\2.0\code\ps\R1_weekly_proc.ps1"

$T = @{
  Frequency="Weekly"
  At="11:00AM"
  DaysOfWeek="Tuesday" , "Thursday"
  #Interval=2
}
$O = @{
  WakeToRun=$true
  StartIfNotIdle=$true
  MultipleInstancePolicy="Queue"
}
Register-ScheduledJob -Trigger $T -ScheduledJobOption $O -Name R1_Weekly_Updates -FilePath $File_Path 

####Monthly
##Every week on Wednesday will  run this Job 
##But script "R1_monthly_proc.ps1" Will test 
##if the day of the month is menor than 8 and if so run the job
## ie. run  only first Wednesday of the Month
$File_Path =  "C:\Users\jeaje\Documents\JA_docs\Dev\Rule1_Investment\Proc\2.0\code\ps\R1_monthly_proc.ps1"
$T = @{
  Frequency="Weekly"
  At="02:00AM"
  DaysOfWeek="Wednesday"
  #Interval=2
}
$O = @{
  WakeToRun=$true
  StartIfNotIdle=$true
  MultipleInstancePolicy="Queue"
}

Register-ScheduledJob -Trigger $T -ScheduledJobOption $O -Name R1_Monthly_Updates -FilePath $File_Path


##utilities
##https://4sysops.com/archives/managing-powershell-scheduled-jobs/
##Get  all shewdules jobs 
Get-ScheduledJob
#Run a Job 
Start-Job -ScriptBlock {R1_Daily_Updates}
#Status  Job rnning
Get-Job
#Remove
Unregister-ScheduledJob -Name R1_Weekly_Updates
