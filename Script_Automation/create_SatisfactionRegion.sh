#!/bin/bash
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/SatisfactionRegion"

# Vérification et préparation du dossier de données
log_message "Checking data directory $data_directory..."
if hdfs dfs -test -d $data_directory; then
    hdfs dfs -rm -r $data_directory
    log_message "Existing data directory removed."
fi
hdfs dfs -mkdir -p $data_directory
hdfs dfs -chmod 777 $data_directory
log_message "Data directory created at $data_directory."

# Déplacer le fichier de données au bon emplacement
log_message "Moving data files..."
hdfs dfs -mv "$DATA_PATH/SatisfactionRegion.txt" $data_directory
hdfs dfs -chown -R cloudera:cloudera $data_directory/SatisfactionRegion.txt
hdfs dfs -chmod 777 $data_directory/*
log_message "Data files moved and permissions set."

# Vérification de l'existence de la table externe
external_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'external_SatisfactionRegion';")
if [[ $external_table_exists == *"external_SatisfactionRegion"* ]]; then
    hive -e "USE healthcare; DROP TABLE external_SatisfactionRegion;"
    log_message "External table SatisfactionRegion dropped successfully."
fi

# Création de la table externe
log_message "Creating external table..."
hive_query_external="USE healthcare;
CREATE EXTERNAL TABLE IF NOT EXISTS external_SatisfactionRegion (
    region_1 STRING,
    taux_satisfaction FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory'
TBLPROPERTIES ('skip.header.line.count'='1');
"

if hive -e "$hive_query_external"; then
    log_message "External table SatisfactionRegion created successfully."
else
    log_message "Failed to create external table SatisfactionRegion."
    exit 1
fi