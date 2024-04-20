#!/bin/bash
# create_diagnostic.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'diagnostic';")

if [[ $table_exists == *"diagnostic"* ]]; then
    log_message "Table diagnostic already exists ..."
    hive -e "USE healthcare; DROP TABLE diagnostic;"
    log_message "Table diagnostic dropped successfully."
fi

log_message "Table diagnostic does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS diagnostic (
    code_diag STRING,
    diagnostic STRING
)
CLUSTERED BY (code_diag) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table diagnostic created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE diagnostic SELECT * FROM diagnostic;"
    log_message "Table diagnostic bucketed and data populated."
else
    log_message "Failed to create and bucket the diagnostic table."
fi