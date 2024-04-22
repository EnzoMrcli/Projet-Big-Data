#!/bin/bash
# create_ConsultationPatient_Etablissement_Temps.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/ConsultationPatient_Etablissement_Temps"

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationPatient_Etablissement_Temps';")

if [[ $table_exists == *"ConsultationPatient_Etablissement_Temps"* ]]; then
    log_message "Table ConsultationPatient_Etablissement_Temps already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationPatient_Etablissement_Temps;"
    log_message "Table ConsultationPatient_Etablissement_Temps dropped successfully."
fi

log_message "Preparing data directory..."
# Assurer que le dossier de données existe et est vide
hdfs dfs -mkdir -p $data_directory
hdfs dfs -rm -r $data_directory/*

# Déplacer le fichier de données au bon emplacement si nécessaire
hdfs dfs -mv "$LOCAL_DATA_PATH/ConsultationPatient_Etablissement_Temps.txt" $data_directory
hdfs dfs -chmod 777 "$data_directory/*"

log_message "Table ConsultationPatient_Etablissement_Temps does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS ConsultationPatient_Etablissement_Temps (
    Id_patient INT,
    raison_sociale_site STRING,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Date_Entree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory';
"

if hive -e "$hive_query"; then
    log_message "Table ConsultationPatient_Etablissement_Temps created successfully."
    hdfs dfs -chmod 777 "$data_directory/*"

    # Peupler la table avec des données
    hive -e "USE healthcare; INSERT OVERWRITE TABLE ConsultationPatient_Etablissement_Temps PARTITION (Date_Entree) SELECT * FROM ConsultationPatient_Etablissement_Temps;"
    log_message "Table ConsultationPatient_Etablissement_Temps partitioned and data populated."
else
    log_message "Failed to create and partition the ConsultationPatient_Etablissement_Temps table."
fi