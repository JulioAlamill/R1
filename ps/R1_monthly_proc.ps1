$Today_day= [int](get-date -Format "dd")

if ( $Today_day  -lt  8 )
{
    cd C:\Users\jeaje\Documents\JA_docs\Dev\Rule1_Investment\Proc\2.0\code\ps
    .\R1_Run_proc.ps1  'monthly'  2
} 
## else { Write-Host "No Work needed Today"  }

