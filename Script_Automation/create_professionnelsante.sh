#!/bin/bash
# create_professionnelsante.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'professionnelsante';")

if [[ $table_exists == *"professionnelsante"* ]]; then
    log_message "Table professionnelsante already exists ..."
    hive -e "USE healthcare; DROP TABLE professionnelsante;"
    log_message "Table professionnelsante dropped successfully."
fi

log_message "Table professionnelsante does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS professionnelsante (
    identifiant INT,
    civilite STRING,
    nom STRING,
    prenom STRING,
    profession STRING,
    type_identifiant STRING,
    code_specialite_1 STRING,
    id_activiteprofessionnelsante INT
)
CLUSTERED BY (identifiant) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table professionnelsante created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE professionnelsante SELECT * FROM professionnelsante;"
    log_message "Table professionnelsante bucketed and data populated."
else
    log_message "Failed to create and bucket the professionnelsante table."
fi