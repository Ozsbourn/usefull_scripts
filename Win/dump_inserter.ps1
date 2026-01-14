# Config
$SQL_DIR = ".\addr_transactions"
$LOG_FILE = "pg_parallel_execution.log"
$DB_HOST = "localhost"
$DB_PORT = "5432"
$DB_NAME = "PASTE_YOUR_DB_NAME_HERE"
$DB_USER = "PASTE_YOUR_DB_USER_HERE"
$DB_PASS = "PASTE_YOUR_DB_PASS_HERE"

# Concurrency settings
$MAX_CONCURRENT = 4
$MAX_RETRIES = 3
$RETRY_DELAY = 5

# Temp log directory
$TMP_LOG_DIR = "C:\Temp\pg_exec_logs"
New-Item -ItemType Directory -Path $TMP_LOG_DIR -Force | Out-Null

# Init of log
"== Parallel execution started ==" | Out-File $LOG_FILE -Append
"Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File $LOG_FILE -Append
"Config:" | Out-File $LOG_FILE -Append
"  - SQL Directory: $SQL_DIR" | Out-File $LOG_FILE -Append
"  - DB: $DB_NAME@$DB_HOST:$DB_PORT" | Out-File $LOG_FILE -Append
"  - Concurency: $MAX_CONCURRENT threads" | Out-File $LOG_FILE -Append
"  - Attempts on one file: $MAX_RETRIES" | Out-File $LOG_FILE -Append
"----------------------------------------" | Out-File $LOG_FILE -Append

function Execute-SQLFile {
    param(
        [string]$sqlFile,
        [int]$taskId
    )
    
    $logFile = Join-Path $TMP_LOG_DIR "task_${taskId}.log"
    "" | Out-File $logFile -Force
    
    "Task $taskId: $sqlFile has begun" | Out-File $logFile -Append
    
    for ($attempt = 1; $attempt -le $MAX_RETRIES; $attempt++) {
        "Task $taskId: attempt $attempt" | Out-File $logFile -Append
        
        try {
            # Using psql.exe w/ Start-Process
            $process = Start-Process psql -Wait -NoNewWindow -PassThru -ArgumentList @(
                "-h", $DB_HOST,
                "-p", $DB_PORT,
                "-U", $DB_USER,
                "-d", $DB_NAME,
                "-q",
                "-v", "ON_ERROR_STOP=1",
                "-f", $sqlFile
            )
            
            if ($process.ExitCode -eq 0) {
                "Task $taskId: success inserted ($sqlFile)" | Out-File $logFile -Append
                return [pscustomobject]@{
                    Status = "SUCCESS"
                    File = $sqlFile
                    TaskId = $taskId
                }
            }
            else {
                "Task $taskId: ERROR (attempt $attempt)" | Out-File $logFile -Append
                Start-Sleep -Seconds $RETRY_DELAY
            }
        }
        catch {
            "Task $taskId: Run error: $_" | Out-File $logFile -Append
        }
    }
    
    "Task $taskId: Insertion FAILED ($sqlFile after $MAX_RETRIES tries)" | Out-File $logFile -Append
    return [pscustomobject]@{
        Status = "FAIL"
        File = $sqlFile
        TaskId = $taskId
    }
}

"Check SQL files existings" | Out-File $LOG_FILE -Append
$sqlFiles = Get-ChildItem -Path $SQL_DIR -Filter "transaction_*.sql" -File

if ($sqlFiles.Count -eq 0) {
    "ERROR: SQL-files was not found at $SQL_DIR" | Out-File $LOG_FILE -Append
    exit 1
}

"Files was found: $($sqlFiles.Count)" | Out-File $LOG_FILE -Append
"Started inserting..." | Out-File $LOG_FILE -Append

$successCount = 0
$failCount = 0
$totalFiles = $sqlFiles.Count

# Concurrent insertion by using ForEach-Object -Parallel
#  Start-ThreadJob for PS lower than 'PowerShell 7'

if ($PSVersionTable.PSVersion.Major -ge 7) {
    $results = $sqlFiles | ForEach-Object -ThrottleLimit $MAX_CONCURRENT -Parallel {
        param($file, $id)
        
        $logFile = Join-Path $using:TMP_LOG_DIR "task_${using:id}.log"
        
        try {
            $process = Start-Process psql -Wait -NoNewWindow -PassThru -ArgumentList @(
                "-h", $using:DB_HOST,
                "-p", $using:DB_PORT,
                "-U", $using:DB_USER,
                "-d", $using:DB_NAME,
                "-q",
                "-v", "ON_ERROR_STOP=1",
                "-f", $file
            )
            
            if ($process.ExitCode -eq 0) {
                return [pscustomobject]@{
                    Status = "SUCCESS"
                    File = $file
                    TaskId = $using:id
                }
            }
        }
        catch {
            return [pscustomobject]@{
                Status = "FAIL"
                File = $file
                TaskId = $using:id
                Error = $_
            }
        }
    } -ArgumentList $sqlFiles.FullName, (1..$totalFiles)
}
else {
    # FOR PS <7.0: Start-ThreadJob
    $jobs = foreach ($i in 0..($sqlFiles.Count-1)) {
        Start-ThreadJob -ThrottleLimit $MAX_CONCURRENT -ArgumentList $sqlFiles[$i].FullName, ($i+1) {
            param($file, $id)
            
            $logFile = Join-Path $using:TMP_LOG_DIR "task_${id}.log"
            
            try {
                $process = Start-Process psql -Wait -NoNewWindow -PassThru -ArgumentList @(
                    "-h", $using:DB_HOST,
                    "-p", $using:DB_PORT,
                    "-U", $using:DB_USER,
                    "-d", $using:DB_NAME,
                    "-q",
                    "-v", "ON_ERROR_STOP=1",
                    "-f", $file
                )
                
                if ($process.ExitCode -eq 0) {
                    return [pscustomobject]@{
                        Status = "SUCCESS"
                        File = $file
                        TaskId = $id
                    }
                }
            }
            catch {
                return [pscustomobject]@{
                    Status = "FAIL"
                    File = $file
                    TaskId = $id
                    Error = $_
                }
            }
        }
    }

    $results = $jobs | Receive-Job -Wait -AutoRemoveJob
}

# Results checking
foreach ($result in $results) {
    if ($result.Status -eq "SUCCESS") {
        $successCount++
        "SUCCESS: $($result.File) (task $($result.TaskId))" | Out-File $LOG_FILE -Append
    }
    else {
        $failCount++
        "ОШИБКА: $($result.File) (task $($result.TaskId))" | Out-File $LOG_FILE -Append
        # Add detailed log for errors
        Get-Content (Join-Path $TMP_LOG_DIR "task_$($result.TaskId).log") | Out-File $LOG_FILE -Append
    }
}

# Final statistic
"==========================================" | Out-File $LOG_FILE -Append
"Execution done" | Out-File $LOG_FILE -Append
"DateTime: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File $LOG_FILE -Append
"----------------------------------------" | Out-File $LOG_FILE -Append
"Statistic:" | Out-File $LOG_FILE -Append
"  - Count of files: $total_files" | Out-File $LOG_FILE -Append
"  - Successed: $successCount"