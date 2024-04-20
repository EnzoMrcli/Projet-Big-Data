#!/bin/bash
# create_hospitalisation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'hospitalisation';")

if [[ $table_exists == *"hospitalisation"* ]]; then
    log_message "Table hospitalisation already exists ..."
    hive -e "USE healthcare; DROP TABLE hospitalisation;"
    log_message "Table hospitalisation dropped successfully."
fi

log_message "Table hospitalisation does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS hospitalisation (
    num_hospitalisation INT,
    suite_diagnostic_consultation STRING,
    jour_hospitalisation INT,
    id_hospitalisationtemps INT,
    identifiant_organisation STRING,  
    id_patient INT,
    code_diag STRING
)
CLUSTERED BY (num_hospitalisation) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table hospitalisation created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE hospitalisation SELECT * FROM hospitalisation;"
    log_message "Table hospitalisation bucketed and data populated."
else
    log_message "Failed to create and bucket the hospitalisation table."
fi