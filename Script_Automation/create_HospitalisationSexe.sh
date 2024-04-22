#!/bin/bash
# create_HospitalisationSexe.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'HospitalisationSexe';")

if [[ $table_exists == *"HospitalisationSexe"* ]]; then
    log_message "Table HospitalisationSexe already exists ..."
    hive -e "USE healthcare; DROP TABLE HospitalisationSexe;"
    log_message "Table HospitalisationSexe dropped successfully."
fi

log_message "Table HospitalisationSexe does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS HospitalisationSexe (
    Sexe STRING,
    nb_hospitalsiation INT
)
CLUSTERED BY (Sexe) INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/HospitalisationSexe';
"
if hive -e "$hive_query"; then
    log_message "Table HospitalisationSexe created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE HospitalisationSexe SELECT * FROM HospitalisationSexe;"
    log_message "Table HospitalisationSexe bucketed and data populated."
else
    log_message "Failed to create and bucket the HospitalisationSexe table."
fi