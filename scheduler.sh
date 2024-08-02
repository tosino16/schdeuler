#!/bin/bash

LOG_PATH="/home/user/logs/scheduler.log"

# Function to log to screen and file
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> $LOG_PATH
}

# Function to validate Oracle table with retry
validate_oracle_table() {
    local table_name=$1
    local count_query=$2
    local max_retries=3
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        log "Validating Oracle table $table_name (Retry: $((retry_count+1)))..."
        local count=$(sqlplus -s user/password@hostname:port/service_name <<EOF
        SET HEADING OFF;
        SET FEEDBACK OFF;
        SET WRAP OFF;
        SET PAGESIZE 0;
        $count_query;
        EXIT;
EOF
)
        if [ $? -eq 0 ]; then
            log "Table $table_name count: $count"
            if [ $count -eq 0 ]; then
                log "Validation for $table_name returned 0 rows. Retrying..."
                sleep 60
                ((retry_count++))
            else
                log "Validation for $table_name succeeded."
                break
            fi
        else
            log "Validation for $table_name failed. Exiting..."
            exit 1
        fi
    done

    if [ $retry_count -eq $max_retries ]; then
        log "Maximum retries reached for $table_name. FAILED..."
        exit 1
    fi
}

# Define tasks as functions
task1() {
    log "Task 1: Extracting data for task1..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task1.py param1 >/dev/null 2>&1
    validate_oracle_table "table1" "SELECT COUNT(*) FROM table1"
}

task2() {
    log "Task 2: Extracting data for task2..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task2.py param2 >/dev/null 2>&1
    validate_oracle_table "table2" "SELECT COUNT(*) FROM table2"
}

task3() {
    log "Task 3: Extracting data for task3..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task3.py param3 >/dev/null 2>&1
    validate_oracle_table "table3" "SELECT COUNT(*) FROM table3"
}

task4() {
    log "Task 4: Extracting data for task4..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task4.py param4 >/dev/null 2>&1
    validate_oracle_table "table4" "SELECT COUNT(*) FROM table4"
}

task5() {
    log "Task 5: Extracting data for task5..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task5.py param5 >/dev/null 2>&1
    validate_oracle_table "table5" "SELECT COUNT(*) FROM table5"
}

task6() {
    log "Task 6: Extracting data for task6..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task6.py param6 >/dev/null 2>&1
    validate_oracle_table "table6" "SELECT COUNT(*) FROM table6"
}

task7() {
    log "Task 7: Extracting data for task7..."
    sleep 15
    validate_oracle_table "table7" "SELECT COUNT(*) FROM table7"
}

task8() {
    log "Task 8: Extracting data for task8..."
    sleep 50
    validate_oracle_table "table8" "SELECT COUNT(*) FROM table8"
}

task9() {
    log "Task 9: Extracting data for task9..."
    sleep 50
    validate_oracle_table "table9" "SELECT COUNT(*) FROM table9"
}

task10() {
    log "Task 10: Extracting data for task10..."
    sleep 5
    validate_oracle_table "table10" "SELECT COUNT(*) FROM table10"
}

task11() {
    log "Task 11: Extracting data for task11..."
    sleep 5
    validate_oracle_table "table11" "SELECT COUNT(*) FROM table11"
}

task12() {
    log "Task 12: Extracting data for task12..."
    sleep 5
    validate_oracle_table "table12" "SELECT COUNT(*) FROM table12"
}

task13() {
    log "Task 13: Extracting data for task13..."
    spark-submit --driver-memory 50G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task13.py >/dev/null 2>&1
    sleep 5
    validate_oracle_table "table13" "SELECT COUNT(*) FROM table13"
}

task14() {
    log "Task 14: Extracting data for task14..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task14.py param14 >/dev/null 2>&1
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task14.py param14_previous_day >/dev/null 2>&1
    sleep 5
    validate_oracle_table "table14" "SELECT COUNT(*) FROM table14"
}

task15() {
    log "Task 15: Extracting data for task15..."
    sleep 2h
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task15.py param15 >/dev/null 2>&1
    sleep 5
    validate_oracle_table "table15" "SELECT COUNT(*) FROM table15"
}

task16() {
    log "Task 16: Running task16..."
    validate_oracle_table "table16" "SELECT COUNT(*) FROM table16"
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task16.py >/dev/null 2>&1
    sleep 30
    date_1=$(date --date="-0 days" +%Y%m%d)
    validate_oracle_table "table16" "SELECT COUNT(*) FROM table16 WHERE date_key = $date_1"
}

task17() {
    log "Task 17: Running task17..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task17.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table17" "SELECT COUNT(*) FROM table17"
}

task18() {
    log "Task 18: Running task18..."
    bash /home/user/prog/task18.sh
    sleep 30
    validate_oracle_table "table18" "SELECT COUNT(*) FROM table18"
}

task19() {
    log "Task 19: Running task19..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task19.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table19" "SELECT COUNT(*) FROM table19"
}

task20() {
    log "Task 20: Running task20..."
    sleep 30
    validate_oracle_table "table20" "SELECT COUNT(*) FROM table20"
}

task21() {
    log "Task 21: Running task21..."
    bash /home/user/prog/task21.sh
    sleep 30
    validate_oracle_table "table21" "SELECT COUNT(*) FROM table21 WHERE EOD_FLAG ='Y'"
    log "Running task21 cleanup..."
    bash /home/user/prog/task21_cleanup.sh param1 param2
}

task22() {
    log "Task 22: Running task22..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task22.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table22" "SELECT COUNT(*) FROM table22"
}

task23() {
    log "Task 23: Running task23..."
    bash /home/user/prog/task23.sh param1 param2
    sleep 30
    validate_oracle_table "table23" "SELECT COUNT(*) FROM table23"
}

task24() {
    log "Task 24: Running task24..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task24.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table24" "SELECT COUNT(*) FROM table24"
}

task25() {
    log "Task 25: Running task25..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task25.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table25" "SELECT COUNT(*) FROM table25"
}

task26() {
    log "Task 26: Running task26..."
    bash /home/user/prog/task26.sh param1 param2 param3 param4
    sleep 30
    validate_oracle_table "table26" "SELECT COUNT(*) FROM table26"
}

task27() {
    log "Task 27: Running task27..."
    spark-submit --driver-memory 30G --num-executors 10 --executor-cores 10 --executor-memory 10G /home/user/prog/task27.py >/dev/null 2>&1
    sleep 30
    validate_oracle_table "table27" "SELECT COUNT(*) FROM table27"
}

task28() {
    log "Task 28: Running task28..."
    bash /home/user/prog/task28.sh
    sleep 30
    validate_oracle_table "table28" "SELECT COUNT(*) FROM table28"
}

task29() {
    log "Task 29: Running task29..."
    validate_oracle_table "table29" "SELECT COUNT(*) FROM table29"
}

task30() {
    log "Task 30: Running task30..."
    bash /home/user/prog/task30.sh param1 param2 param3 param4 param5 param6
    sleep 30
    validate_oracle_table "table30" "SELECT COUNT(*) FROM table30"
}

task31() {
    log "Task 31: Running task31..."
    sleep 30
    validate_oracle_table "table31" "SELECT COUNT(*) FROM table31"
}

task32() {
    log "Task 32: Running task32..."
    bash /home/user/prog/task32.sh
    sleep 30
    validate_oracle_table "table32" "SELECT COUNT(*) FROM table32"
}

task33() {
    log "Task 33: Running task33..."
    sleep 30
    validate_oracle_table "table33" "SELECT COUNT(*) FROM table33"
}

# Execute tasks in parallel with dependency checks
execute_parallel() {
    log "Executing tasks in parallel with dependency checks..."

    task1 &
    pid1=$!

    task2 &
    pid2=$!

    task3 &
    pid3=$!

    task4 &
    pid4=$!

    task5 &
    pid5=$!

    task6 &
    pid6=$!

    task7 &
    pid7=$!

    task8 &
    pid8=$!

    task9 &
    pid9=$!

    task10 &
    pid10=$!

    task11 &
    pid11=$!

    task12 &
    pid12=$!

    task13 &
    pid13=$!

    task14 &
    pid14=$!

    task15 &
    pid15=$!

    task16 &
    pid16=$!

    task17 &
    pid17=$!

    task18 &
    pid18=$!

    task19 &
    pid19=$!

    task20 &
    pid20=$!

    task21 &
    pid21=$!

    task22 &
    pid22=$!

    task23 &
    pid23=$!

    task24 &
    pid24=$!

    task25 &
    pid25=$!

    task26 &
    pid26=$!

    task27 &
    pid27=$!

    task28 &
    pid28=$!

    task29 &
    pid29=$!

    task30 &
    pid30=$!

    task31 &
    pid31=$!

    task32 &
    pid32=$!

    task33 &
    pid33=$!

    wait $pid1 $pid2 $pid3 $pid4 $pid5 $pid6 $pid7 $pid8 $pid9 $pid10 $pid11 $pid12 $pid13 $pid14 $pid15 $pid16 $pid17 $pid18 $pid19 $pid20 $pid21 $pid22 $pid23 $pid24 $pid25 $pid26 $pid27 $pid28 $pid29 $pid30 $pid31 $pid32 $pid33

    if [ $? -eq 0 ]; then
        log "All tasks completed successfully."
    else
        log "One or more tasks failed."
        exit 1
    fi
}

execute_parallel
