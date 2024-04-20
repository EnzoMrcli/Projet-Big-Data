#!/bin/bash
# create_consultation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'consultation';")

if [[ $table_exists == *"consultation"* ]]; then
    log_message "Table consultation already exists ..."
    hive -e "USE healthcare; DROP TABLE consultation;"
    log_message "Table consultation dropped successfully."
fi

log_message "Table consultation does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS consultation (
    num_consultation INT,
    motif STRING,
    id_tempsconsultation INT,
    id_professionnelsante INT,
    id_mut INT,
    code_diag STRING,
    id_salle INT
)
CLUSTERED BY (id_patient) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table consultation created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE consultation SELECT * FROM consultation;"
    log_message "Table consultation bucketed and data populated."
else
    log_message "Failed to create and bucket the consultation table."
fi