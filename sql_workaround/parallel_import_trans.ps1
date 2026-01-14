param (
    [string]$Dir = "transactions",
    [int]$Parallel = 4,
    [string]$PsqlArgs = "-U user -d db"
)

Get-ChildItem $Dir\transaction_*.sql |
Sort-Object Name |
ForEach-Object -Parallel {
    Write-Host "Import $($_.Name)"
    psql $using:PsqlArgs -f $_.FullName
} -ThrottleLimit $Parallel
