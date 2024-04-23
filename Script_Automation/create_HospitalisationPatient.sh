#!/bin/bash
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/HospitalisationPatient"

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
hdfs dfs -mv "$DATA_PATH/HospitalisationPatient.txt" $data_directory
hdfs dfs -chown -R cloudera:cloudera $data_directory/HospitalisationPatient.txt
hdfs dfs -chmod 777 $data_directory/*
log_message "Data files moved and permissions set."

# Vérification de l'existence de la table externe
external_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'external_hospitalisationpatient';")
if [[ $external_table_exists == *"external_hospitalisationpatient"* ]]; then
    hive -e "USE healthcare; DROP TABLE external_hospitalisationpatient;"
    log_message "External table external_hospitalisationpatient dropped successfully."
fi

# Création de la table externe
log_message "Creating external table..."
hive_query_external="USE healthcare;
CREATE EXTERNAL TABLE IF NOT EXISTS external_HospitalisationPatient (
    Id_patient INT,
    Date_Entree STRING,
    Jour_Hospitalisation INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory'
TBLPROPERTIES ('skip.header.line.count'='1');
"

if hive -e "$hive_query_external"; then
    log_message "External table external_HospitalisationPatient created successfully."
else
    log_message "Failed to create external table external_HospitalisationPatient."
    exit 1
fi

# Configuration des paramètres de partitionnement et de bucketing
hive -e "USE healthcare; SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict; SET hive.conf.validation=false; SET hive.enforce.bucketing=true;"

# Création de la table interne avec partitionnement et bucketing
log_message "Creating internal table with partition and buckets..."
hive_query_internal="USE healthcare; CREATE TABLE IF NOT EXISTS HospitalisationPatient (
    Id_patient INT,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Date_Entree STRING)
CLUSTERED BY (Id_patient) INTO 4 BUCKETS
STORED AS ORC;
"

if hive -e "$hive_query_internal"; then
    log_message "Internal table HospitalisationPatient created successfully with partition and buckets."

    # Insertion des données dans la table interne depuis la table externe
    log_message "Inserting data into internal table from external table..."
    hive -e "USE healthcare;
    INSERT OVERWRITE TABLE HospitalisationPatient PARTITION (Date_Entree)
    SELECT Id_patient, Jour_Hospitalisation, Date_Entree FROM external_HospitalisationPatient;
    "
    log_message "Data inserted into internal table HospitalisationPatient with partition and buckets."
else
    log_message "Failed to create the internal table with partition and buckets."
    exit 1
fi