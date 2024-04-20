#!/bin/bash
# create_patient.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'patient';")

if [[ $table_exists == *"patient"* ]]; then
    log_message "Table patient already exists ..."
    hive -e "USE healthcare; DROP TABLE patient;"
    log_message "Table patient dropped successfully."

log_message "Table patient does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS patient (
    id_patient INT,
    nom STRING,
    prenom STRING,
    sexe STRING,
    adresse STRING,
    ville STRING,
    code_postal STRING,
    email STRING,
    tel STRING,
    date STRING,
    age INT,
    num_secu STRING,
    groupe_sanguin STRING,
    poids DOUBLE,
    taille DOUBLE
)
PARTITIONED BY (pays STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table patient created successfully."
else
    log_message "Failed to create the patient table."
fi