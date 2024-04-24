#!/bin/bash
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Définir le chemin du nouveau dossier de données
data_directory="$DATA_PATH/ConsultationPatient_Etablissement_Temps"

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
hdfs dfs -mv "$DATA_PATH/ConsultationPatient_Etablissement_Temps.txt" $data_directory
hdfs dfs -chown -R cloudera:cloudera $data_directory/ConsultationPatient_Etablissement_Temps.txt
hdfs dfs -chmod 777 $data_directory/*
log_message "Data files moved and permissions set."

# Vérification de l'existence de la table externe
external_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'external_consultationpatient_etablissement_temps';")
if [[ $external_table_exists == *"external_consultationpatient_etablissement_temps"* ]]; then
    hive -e "USE healthcare; DROP TABLE external_consultationpatient_etablissement_temps;"
    log_message "External table external_consultationpatient_etablissement_temps dropped successfully."
fi

# Vérification de l'existence de la table interne
internal_table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationPatient_Etablissement_Temps';")

if [[ $internal_table_exists == *"ConsultationPatient_Etablissement_Temps"* ]]; then
    log_message "Table ConsultationPatient_Etablissement_Temps already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationPatient_Etablissement_Temps;"
    log_message "Table ConsultationPatient_Etablissement_Temps dropped successfully."
fi

# Création de la table externe
log_message "Creating external table..."
hive_query_external="USE healthcare;
CREATE EXTERNAL TABLE IF NOT EXISTS external_ConsultationPatient_Etablissement_Temps (
    Id_patient INT,
    raison_sociale_site STRING,
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
    log_message "External table external_ConsultationPatient_Etablissement_Temps created successfully."
else
    log_message "Failed to create external table external_ConsultationPatient_Etablissement_Temps."
    exit 1
fi

# Création de la table interne avec partitionnement et bucketing
log_message "Creating internal table with partition and buckets..."
hive_query_internal="USE healthcare; CREATE TABLE IF NOT EXISTS ConsultationPatient_Etablissement_Temps (
    Id_patient INT,
    raison_sociale_site STRING,
    Jour_Hospitalisation INT
)
PARTITIONED BY (Date_Entree STRING)
CLUSTERED BY (Id_patient) INTO 2 BUCKETS
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE;
"

if hive -e "$hive_query_internal"; then
    log_message "Internal table ConsultationPatient_Etablissement_Temps created successfully with partition and buckets."

    # Insertion des données dans la table interne depuis la table externe
    log_message "Inserting data into internal table from external table..."
    hive -e "USE healthcare; SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict; SET hive.conf.validation=false; SET hive.enforce.bucketing=true;
    INSERT OVERWRITE TABLE ConsultationPatient_Etablissement_Temps PARTITION (Date_Entree)
    SELECT Id_patient, raison_sociale_site, Jour_Hospitalisation, Date_Entree FROM external_ConsultationPatient_Etablissement_Temps;
    "
    log_message "Data inserted into internal table ConsultationPatient_Etablissement_Temps with partition and buckets."
else
    log_message "Failed to create the internal table with partition and buckets."
    exit 1
fi