$StopWatch = new-object system.diagnostics.stopwatch # stopwatch start 
$StopWatch.Start()
$Job_started = ('Job started at: ' + (get-date).ToString('T') )
Write-Host $Job_started 
#Paameters 
$process=$args[0]
$tickers_list=$args[1]

##Dealing with new file PAth, names, creation 
cd C:\Users\jeaje\Documents\JA_docs\Dev\Rule1_Investment\Proc\2.0\code
$logfile_path =  ".\ps\logs\" + $process + "\" 
$logfile_name = "R1_" + $process +"_"+   ((get-date).ToString('yyyyMMdd')) + ".txt"
$logfile_path_full =  $logfile_path  + $logfile_name 

New-Item -Path $logfile_path -Name $logfile_name -ItemType "file" -Force -Value $Job_started
#time thing 
#Set-Content $logfile_path_full  -Value $Job_started -Encoding ascii

## Run the script with PArameter
Add-Content $logfile_path_full '---------------'
C:\Anaconda3\python .\py\Run_R1_Script.py  $process   $tickers_list | Out-File -FilePath $logfile_path_full -Append -Encoding ascii 
Add-Content $logfile_path_full '---------------'

#time thing 
$Job_Finished = ('Job Finished at: '+ (get-date).ToString('T'))
Add-Content $logfile_path_full  $Job_Finished
Write-Host $Job_Finished
Add-Content  $logfile_path_full ('Job Time: ' + $StopWatch.Elapsed.ToString()) # stopwatch  Ends 
#end
Start-Sleep -s 5 ## Just to let you see the screen
