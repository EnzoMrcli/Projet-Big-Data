#!/bin/bash
# create_ConsultationPatient_Diagnostic_Temps.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationPatient_Diagnostic_Temps';")

if [[ $table_exists == *"ConsultationPatient_Diagnostic_Temps"* ]]; then
    log_message "Table ConsultationPatient_Diagnostic_Temps already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationPatient_Diagnostic_Temps;"
    log_message "Table ConsultationPatient_Diagnostic_Temps dropped successfully."
fi

log_message "Table ConsultationPatient_Diagnostic_Temps does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS ConsultationPatient_Diagnostic_Temps (
    Id_patient INT,
    Diagnostic STRING,
    Date STRING
)
CLUSTERED BY (Id_patient) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/ConsultationPatient_Diagnostic_Temps';
"

if hive -e "$hive_query"; then
    log_message "Table ConsultationPatient_Diagnostic_Temps created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE ConsultationPatient_Diagnostic_Temps SELECT * FROM ConsultationPatient_Diagnostic_Temps;"
    log_message "Table ConsultationPatient_Diagnostic_Temps bucketed and data populated."
else
    log_message "Failed to create and bucket the ConsultationPatient_Diagnostic_Temps table."
fi