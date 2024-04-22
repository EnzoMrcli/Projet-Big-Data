#!/bin/bash
# create_HospitalisationPatient_Diagnostic.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'HospitalisationPatient_Diagnostic';")

if [[ $table_exists == *"HospitalisationPatient_Diagnostic"* ]]; then
    log_message "Table HospitalisationPatient_Diagnostic already exists ..."
    hive -e "USE healthcare; DROP TABLE HospitalisationPatient_Diagnostic;"
    log_message "Table HospitalisationPatient_Diagnostic dropped successfully."
fi

log_message "Table HospitalisationPatient_Diagnostic does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS HospitalisationPatient_Diagnostic (
    Id_patient INT,
    Diagnostic STRING,
    Date_Entree STRING,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Date_Entree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/HospitalisationPatient_Diagnostic';
"

if hive -e "$hive_query"; then
    log_message "Table HospitalisationPatient_Diagnostic created and partitioned successfully."
    # Peupler la table avec des données
    hive -e "USE healthcare; INSERT OVERWRITE TABLE HospitalisationPatient_Diagnostic PARTITION (Date_Entree) SELECT * FROM HospitalisationPatient_Diagnostic;"
    log_message "Table HospitalisationPatient_Diagnostic partitioned and data populated."
else
    log_message "Failed to create and partition the HospitalisationPatient_Diagnostic table."
fi