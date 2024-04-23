#!/bin/bash
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/ConsultationPatient_Diagnostic_Temps"

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
hdfs dfs -mv "$DATA_PATH/ConsultationPatient_Diagnostic_Temps.txt" $data_directory
hdfs dfs -chown -R cloudera:cloudera $data_directory/ConsultationPatient_Diagnostic_Temps.txt
hdfs dfs -chmod 777 $data_directory/*
log_message "Data files moved and permissions set."

# Vérification de l'existence de la table externe
external_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'external_consultationpatient_diagnostic_temps';")
if [[ $external_table_exists == *"external_consultationpatient_diagnostic_temps"* ]]; then
    hive -e "USE healthcare; DROP TABLE external_consultationpatient_diagnostic_temps;"
    log_message "External table external_consultationpatient_diagnostic_temps dropped successfully."
fi

# Vérification de l'existence de la table interne
internal_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationPatient_Diagnostic_Temps';")

if [[ $internal_table_exists == *"ConsultationPatient_Diagnostic_Temps"* ]]; then
    log_message "Table ConsultationPatient_Diagnostic_Temps already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationPatient_Diagnostic_Temps;"
    log_message "Table ConsultationPatient_Diagnostic_Temps dropped successfully."
fi

# Création de la table externe
log_message "Creating external table..."
hive_query_external="USE healthcare;
CREATE EXTERNAL TABLE IF NOT EXISTS external_ConsultationPatient_Diagnostic_Temps (
    Id_patient INT,
    Diagnostic STRING,
    Date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '$data_directory'
TBLPROPERTIES ('skip.header.line.count'='1');
"

if hive -e "$hive_query_external"; then
    log_message "External table external_ConsultationPatient_Diagnostic_Temps created successfully."
else
    log_message "Failed to create external table external_ConsultationPatient_Diagnostic_Temps."
    exit 1
fi

hive -e "USE healthcare; SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict; SET hive.conf.validation=false; SET hive.enforce.bucketing=true;"

# Création de la table interne avec partitionnement et bucketing
log_message "Creating internal table with partition and buckets..."
hive_query_internal="USE healthcare; CREATE TABLE IF NOT EXISTS ConsultationPatient_Diagnostic_Temps (
    Id_patient INT,
    Diagnostic STRING
)
PARTITIONED BY (Date DATE)
CLUSTERED BY (Id_patient) INTO 4 BUCKETS
STORED AS ORC;
"

if hive -e "$hive_query_internal"; then
    log_message "Internal table ConsultationPatient_Diagnostic_Temps created successfully with partition and buckets."

    # Insertion des données dans la table interne depuis la table externe
    log_message "Inserting data into internal table from external table..."
    hive -e "USE healthcare;
    INSERT OVERWRITE TABLE ConsultationPatient_Diagnostic_Temps PARTITION (Date)
    SELECT Id_patient, Diagnostic, Date FROM external_ConsultationPatient_Diagnostic_Temps;
    "
    log_message "Data inserted into internal table ConsultationPatient_Diagnostic_Temps with partition and buckets."
else
    log_message "Failed to create the internal table with partition and buckets."
    exit 1
fi