#!/bin/bash
# create_HospitalisationPatient.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/HospitalisationPatient"

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'HospitalisationPatient';")

if [[ $table_exists == *"HospitalisationPatient"* ]]; then
    log_message "Table HospitalisationPatient already exists ..."
    hive -e "USE healthcare; DROP TABLE HospitalisationPatient;"
    log_message "Table HospitalisationPatient dropped successfully."
fi

log_message "Preparing data directory..."
# Assurer que le dossier de données existe et est vide
hdfs dfs -mkdir -p $data_directory
hdfs dfs -rm -r $data_directory/*

# Déplacer le fichier de données au bon emplacement si nécessaire
hdfs dfs -mv "$LOCAL_DATA_PATH/HospitalisationPatient.txt" $data_directory
hdfs dfs -chmod 777 "$data_directory/*"

log_message "Table HospitalisationPatient does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS HospitalisationPatient (
    Date_Entree STRING,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Id_patient INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory';
"

if hive -e "$hive_query"; then
    log_message "Table HospitalisationPatient created and partitioned successfully."
    hdfs dfs -chmod 777 "$data_directory/*"

    # Peupler la table avec des données
    hive -e "USE healthcare; INSERT OVERWRITE TABLE HospitalisationPatient PARTITION (Id_patient) SELECT * FROM HospitalisationPatient;"
    log_message "Table HospitalisationPatient partitioned and data populated."
else
    log_message "Failed to create and partition the HospitalisationPatient table."
fi