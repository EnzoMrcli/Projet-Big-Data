#!/bin/bash

# Définition des chemins d'accès
export DATA_PATH="/user/hive/data"
export LOCAL_DATA_PATH="/user/cloudera/DatawareHouse"  
export LOCAL_SCRIPT_PATH="/home/cloudera/script_automatisation/scripts"
export LOCAL_LOGS_PATH="/home/cloudera/script_automatisation/logs"
export LOCAL_LOG_FILE="$LOCAL_LOGS_PATH/logs.txt"  

# Fonction pour loguer les messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOCAL_LOG_FILE"
}

setup_dir() {
    # Création des dossiers locaux pour logs s'ils n'existent pas
    chmod -R 777 $LOCAL_SCRIPT_PATH
    if [ ! -d "$LOCAL_LOGS_PATH" ]; then
        mkdir -p $LOCAL_LOGS_PATH
        chmod -R 777 $LOCAL_LOGS_PATH
    fi
}

# Création de la base de données si elle n'existe pas
create_healthcare_db() {
    local db_exists=$(hive -e "SHOW DATABASES LIKE 'healthcare';")
    if [[ -z "$db_exists" ]]; then
        hive -e "CREATE DATABASE healthcare;"
        log_message "Database healthcare created."
    else
        log_message "Database healthcare already exists."
    fi
}

# Configuration initiale pour HDFS
setup_hdfs() {
    hdfs dfs -mkdir -p $DATA_PATH
    hdfs dfs -chmod 777 $DATA_PATH
    log_message "HDFS directory setup and permissions set."
}

# Configuration de Hive pour le partitionnement dynamique et le bucketing
setup_hive_config() {
    hive -e "SET hive.exec.dynamic.partition=true;"
    hive -e "SET hive.exec.dynamic.partition.mode=nonstrict;"
    hive -e "SET hive.enforce.bucketing=true;"
    log_message "Hive configurations for dynamic partitioning and bucketing set."
}

# Déplacement des fichiers .txt vers HDFS
move_txt_files() {
    found_files=false
    for file in $LOCAL_DATA_PATH/*.txt; do
        filename=$(basename "$file")
        if [[ "$filename" =~ ^(Consultation.txt|Deces.txt|Diagnostic.txt|EtablissementSante.txt|Hospitalisation.txt|Patient.txt|ProfessionnelSante.txt|TempsConsultation.txt|TempsHospitalisation.txt)$ ]]; then
            hdfs dfs -mv "$file" "$DATA_PATH/"
            found_files=true
        fi
    done
    if [ "$found_files" = true ]; then
        log_message "Eligible files have been moved to HDFS."
    else
        log_message "No new eligible files found to move to HDFS."
    fi
}

# Exécuter les fonctions lors de l'appel direct du script
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    setup_dir
    setup_hdfs
    setup_hive_config
    move_txt_files
    log_message "Initialisation completed successfully."
fi