#!/bin/bash
# create_tempsconsultation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'tempsconsultation';")

if [[ $table_exists == *"tempsconsultation"* ]]; then
    log_message "Table tempsconsultation already exists ..."
    hive -e "USE healthcare; DROP TABLE tempsconsultation;"
    log_message "Table tempsconsultation dropped successfully."

log_message "Table tempsconsultation does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS tempsconsultation (
    id_tempsconsultation INT,
    num_consultation INT,
    heure_debut STRING,
    heure_fin STRING
)
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table tempsconsultation created successfully."
else
    log_message "Failed to create the tempsconsultation table."
fi