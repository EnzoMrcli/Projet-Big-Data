#!/bin/bash
# create_tempshospitalisation.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'tempshospitalisation';")

if [[ $table_exists == *"tempshospitalisation"* ]]; then
    log_message "Table tempshospitalisation already exists ..."
    hive -e "USE healthcare; DROP TABLE tempshospitalisation;"
    log_message "Table tempshospitalisation dropped successfully."

log_message "Table tempshospitalisation does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS tempshospitalisation (
    id_hospitalisationtemps INT,
    num_hospitalisation INT
)
PARTITIONED BY (date_entree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table tempshospitalisation created successfully."
else
    log_message "Failed to create the tempshospitalisation table."
fi