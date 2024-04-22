#!/bin/bash
# create_ConsultationPatient_Etablissement_Temps.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationPatient_Etablissement_Temps';")

if [[ $table_exists == *"ConsultationPatient_Etablissement_Temps"* ]]; then
    log_message "Table ConsultationPatient_Etablissement_Temps already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationPatient_Etablissement_Temps;"
    log_message "Table ConsultationPatient_Etablissement_Temps dropped successfully."
fi

log_message "Table ConsultationPatient_Etablissement_Temps does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS ConsultationPatient_Etablissement_Temps (
    Id_patient INT,
    raison_sociale_site STRING,
    Date_Entree STRING,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Date_Entree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/ConsultationPatient_Etablissement_Temps';
"

if hive -e "$hive_query"; then
    log_message "Table ConsultationPatient_Etablissement_Temps created successfully."
else
    log_message "Failed to create the ConsultationPatient_Etablissement_Temps table."
fi