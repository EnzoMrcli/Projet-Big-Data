#!/bin/bash
# create_SatisfactionRegion.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/SatisfactionRegion"

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'SatisfactionRegion';")

if [[ $table_exists == *"SatisfactionRegion"* ]]; then
    log_message "Table SatisfactionRegion already exists ..."
    hive -e "USE healthcare; DROP TABLE SatisfactionRegion;"
    log_message "Table SatisfactionRegion dropped successfully."
fi

log_message "Preparing data directory..."
# Assurer que le dossier de données existe et est vide
hdfs dfs -mkdir -p $data_directory
hdfs dfs -rm -r $data_directory/*

# Déplacer le fichier de données au bon emplacement si nécessaire
hdfs dfs -mv "$LOCAL_DATA_PATH/SatisfactionRegion.txt" $data_directory
hdfs dfs -chmod 777 "$data_directory/*"

log_message "Table SatisfactionRegion does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS SatisfactionRegion (
    region_1 STRING,
    taux_satisfaction DOUBLE
)
CLUSTERED BY (region_1) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '$data_directory';
"

if hive -e "$hive_query"; then
    log_message "Table SatisfactionRegion created and bucketed successfully."
    hdfs dfs -chmod 777 "$data_directory/*"

    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE SatisfactionRegion SELECT * FROM SatisfactionRegion;"
    log_message "Table SatisfactionRegion bucketed and data populated."
else
    log_message "Failed to create and bucket the SatisfactionRegion table."
fi