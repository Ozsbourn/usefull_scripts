#!/usr/bin/env bash
set -euo pipefail

# ================== args ==================
INPUT=""
OUTDIR="transactions"
MAX_PARAMS=65000
DRY_RUN=0
RESUME=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --resume)  RESUME=1 ;;
        *) INPUT="$arg" ;;
    esac
done

if [[ -z "$INPUT" ]]; then
    echo "Usage: $0 dump.sql [--dry-run] [--resume]"
    exit 1
fi

mkdir -p "$OUTDIR"

# ================== resume ==================
tx=1
if (( RESUME == 1 )); then
    last=$(ls "$OUTDIR"/transaction_*.sql 2>/dev/null \
        | sed -E 's/.*transaction_([0-9]+)\.sql/\1/' \
        | sort -n | tail -1)

    if [[ -n "$last" ]]; then
        tx=$((10#$last + 1))
        echo "Resume from transaction $tx"
    fi
fi

current_params=0
current_batch=()

# ================== helpers ==================
save_transaction() {
    if [ ${#current_batch[@]} -eq 0 ]; then
        return
    fi

    echo "Transaction $tx : $current_params params"

    if (( DRY_RUN == 0 )); then
        file=$(printf "%s/transaction_%04d.sql" "$OUTDIR" "$tx")
        {
            echo "BEGIN;"
            for l in "${current_batch[@]}"; do
                echo "$l"
            done
            echo "COMMIT;"
        } > "$file"
    fi

    tx=$((tx + 1))
    current_params=0
    current_batch=()
}

count_columns_in_tuple() {
    local text="$1"
    local depth=0 in_string=0 count=1
    local i c

    for ((i=0; i<${#text}; i++)); do
        c="${text:i:1}"

        if [[ "$c" == "'" ]]; then
            ((in_string ^= 1))
            continue
        fi
        (( in_string )) && continue

        [[ "$c" == "(" ]] && ((depth++))
        [[ "$c" == ")" ]] && ((depth--))

        if [[ "$c" == "," && $depth -eq 0 ]]; then
            ((count++))
        fi
    done

    echo "$count"
}

count_values_groups() {
    local sql="$1"
    local part="${sql#*VALUES}"
    local depth=0 in_string=0 groups=0
    local i c

    for ((i=0; i<${#part}; i++)); do
        c="${part:i:1}"

        if [[ "$c" == "'" ]]; then
            ((in_string ^= 1))
            continue
        fi
        (( in_string )) && continue

        if [[ "$c" == "(" ]]; then
            (( depth == 0 )) && ((groups++))
            ((depth++))
        elif [[ "$c" == ")" ]]; then
            ((depth--))
        fi
    done

    echo "$groups"
}

# ================== main ==================
inside_insert=0
insert_buffer=""

pv "$INPUT" | while IFS= read -r line; do

    if (( inside_insert == 0 )); then
        if [[ "$line" =~ ^[[:space:]]*INSERT[[:space:]]+INTO ]]; then
            inside_insert=1
            insert_buffer="$line"
        else
            current_batch+=("$line")
        fi
    else
        insert_buffer+=$'\n'"$line"
    fi

    if (( inside_insert )) && [[ "$line" == *";"* ]]; then
        inside_insert=0

        tuple=$(sed -nE 's/.*VALUES[[:space:]]*\((.*)\).*/\1/p' <<< "$insert_buffer")
        cols=$(count_columns_in_tuple "$tuple")
        groups=$(count_values_groups "$insert_buffer")
        params=$((cols * groups))

        if (( params > MAX_PARAMS )); then
            echo "ERROR: single INSERT exceeds limit"
            exit 1
        fi

        if (( current_params + params > MAX_PARAMS )); then
            save_transaction
        fi

        current_batch+=("$insert_buffer")
        current_params=$((current_params + params))
        insert_buffer=""
    fi
done

save_transaction
