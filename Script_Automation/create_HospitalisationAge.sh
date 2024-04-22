#!/bin/bash
# create_HospitalisationAge.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'HospitalisationAge';")

if [[ $table_exists == *"HospitalisationAge"* ]]; then
    log_message "Table HospitalisationAge already exists ..."
    hive -e "USE healthcare; DROP TABLE HospitalisationAge;"
    log_message "Table HospitalisationAge dropped successfully."
fi

log_message "Table HospitalisationAge does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS HospitalisationAge (
    Age INT,
    nb_hospitalisation INT
)
CLUSTERED BY (Age) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/HospitalisationAge';
"

if hive -e "$hive_query"; then
    log_message "Table HospitalisationAge created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE HospitalisationAge SELECT * FROM HospitalisationAge;"
    log_message "Table HospitalisationAge bucketed and data populated."
else
    log_message "Failed to create and bucket the HospitalisationAge table."
fi