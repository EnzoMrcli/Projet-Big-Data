#!/bin/bash
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/HospitalisationSexe"

# Vérification et préparation du dossier de données
log_message "Checking data directory $data_directory..."
if hdfs dfs -test -d $data_directory; then
    hdfs dfs -rm -r $data_directory
    log_message "Existing data directory removed."
fi
hdfs dfs -mkdir -p $data_directory
log_message "Data directory created at $data_directory."

# Déplacer le fichier de données au bon emplacement
log_message "Moving data files..."
hdfs dfs -mv "$DATA_PATH/HospitalisationSexe.txt" $data_directory
hdfs dfs -chown -R cloudera:cloudera $data_directory/HospitalisationSexe.txt
hdfs dfs -chmod 777 $data_directory/*
log_message "Data files moved and permissions set."

# Vérification de l'existence de la table externe
external_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'HospitalisationSexe';")
if [[ $external_table_exists == *"HospitalisationSexe"* ]]; then
    hive -e "USE healthcare; DROP TABLE HospitalisationSexe;"
    log_message "External table HospitalisationSexe dropped successfully."
fi

# Création de la table externe
log_message "Creating external table..."
hive_query_external="USE healthcare;
CREATE EXTERNAL TABLE IF NOT EXISTS HospitalisationSexe (
    Sexe STRING,
    nb_hospitalisation INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory';
"

if hive -e "$hive_query_external"; then
    log_message "External table HospitalisationSexe created successfully."
else
    log_message "Failed to create external table HospitalisationSexe."
    exit 1
fi