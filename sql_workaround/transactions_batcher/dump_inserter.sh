#!/bin/bash

# === TODO: replace it to config at /.pgpass maybe ===
SQL_DIR="./addr_transactions"             
LOG_FILE="pg_parallel_execution.log"      
DB_HOST="localhost"                       
DB_PORT="5432"                           
DB_NAME="YOUR_DB_NAME"                       
DB_USER="YOUR_DB_USER"                       
DB_PASS="YOUR_DB_PASS"                       

# Concurrency params
MAX_CONCURRENT=4                        # max concurrent connection
MAX_RETRIES=3                           # max retries on single file
RETRY_DELAY=5                           # delay between retries (sec)

# temp catalog for logs
TMP_LOG_DIR="/tmp/pg_exec_logs"
mkdir -p "$TMP_LOG_DIR"

# === Log Initialization ===
{
    echo "=== Parallel execution started ==="
    echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Configuration:"
    echo "  - Directory SQL: $SQL_DIR"
    echo "  - DB: $DB_NAME@$DB_HOST:$DB_PORT"
    echo "  - Concurency: $MAX_CONCURRENT threads"
    echo "  - Attempts on one file: $MAX_RETRIES"
    echo "----------------------------------------"
} > "$LOG_FILE"

execute_sql_file() {
    local sql_file="$1"
    local task_id="$2"
    
    local log_file="$TMP_LOG_DIR/task_${task_id}.log"
    > "$log_file"
    
    echo "Task $task_id: serving started $sql_file" >> "$log_file"
    
    for attempt in $(seq 1 $MAX_RETRIES); do
        echo "Task $task_id: attempt $attempt" >> "$log_file"
        
        PGPASSWORD="$DB_PASS" psql \
            -h "$DB_HOST" \
            -p "$DB_PORT" \
            -U "$DB_USER" \
            -d "$DB_NAME" \
            -q \
            -v ON_ERROR_STOP=1 \
            -f "$sql_file" \
            >> "$log_file" 2>&1
        
        if [[ $? -eq 0 ]]; then
            echo "Task $task_id: SUCCESS ($sql_file)" >> "$log_file"
            echo "SUCCESS:$sql_file:$task_id"
            return 0
        else
            echo "Task $task_id: ERROR (atempt $attempt)" >> "$log_file"
            if [[ $attempt -lt $MAX_RETRIES ]]; then
                sleep $RETRY_DELAY
            fi
        fi
    done
    
    echo "Task $task_id: FAILED ($sql_file after $MAX_RETRIES attempts)" >> "$log_file"
    echo "FAIL:$sql_file:$task_id"
    return 1
}

echo "Searching SQL files" >> "$LOG_FILE"
sql_files=("$SQL_DIR"/transaction_*.sql)

if [[ ${#sql_files[@]} -eq 0 ]]; then
    echo "ERROR: SQL‑files does not found at $SQL_DIR" >> "$LOG_FILE"
    exit 1
fi

echo "Found files: ${#sql_files[@]}" >> "$LOG_FILE"
echo "Parallel execution..." >> "$LOG_FILE"

success_count=0
fail_count=0
total_files=${#sql_files[@]}

# GNU parallel for concurrent execution
export -f execute_sql_file
export DB_PASS DB_HOST DB_PORT DB_USER DB_NAME MAX_RETRIES RETRY_DELAY TMP_LOG_DIR

parallel -j "$MAX_CONCURRENT" --link \
    'result=$(execute_sql_file {1} {2}); echo "$result"' \
    ::: "${sql_files[@]}" \
    ::: $(seq 1 $total_files) |
    while IFS=: read -r status file task_id; do
        if [[ "$status" == "SUCCESS" ]]; then
            ((success_count++))
            echo "SUCCESS: $file (task $task_id)" >> "$LOG_FILE"
        else
            ((fail_count++))
            echo "ERROR: $file (task $task_id)" >> "$LOG_FILE"
            # cope detail report of error
            cat "$TMP_LOG_DIR/task_${task_id}.log" >> "$LOG_FILE"
        fi
    done

{
    echo "========================================"
    echo "EXECUTION DONE"
    echo "DATE: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "----------------------------------------"
    echo "RESUME:"
    echo "  - Total files: $total_files"
    echo "  - Successed: $success_count"
    echo "  - Errors: $fail_count"
    echo "  - Success rate: $((100 * success_count / total_files))%"
    echo "========================================"
} >> "$LOG_FILE"

echo "Done. You can log info at $LOG_FILE"
echo "More detail log at $TMP_LOG_DIR"

#  (optional) TODO: maybe change ot to settingup by command flag?
# rm -rf "$TMP_LOG_DIR"

