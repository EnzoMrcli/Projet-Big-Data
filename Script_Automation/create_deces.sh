#!/bin/bash
# create_deces.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'deces';")

if [[ $table_exists == *"deces"* ]]; then
    log_message "Table deces already exists ..."
    hive -e "USE healthcare; DROP TABLE deces;"
    log_message "Table deces dropped successfully."
fi

log_message "Table deces does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS deces (
    id_deces INT,
    nom STRING,
    prenom STRING,
    sexe STRING,
    code_lieu_naissance STRING,
    lieu_naissance STRING,
    pays_naissance STRING,
    code_lieu_deces STRING,
    numero_acte_deces STRING
)
CLUSTERED BY (id_deces) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table deces created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE deces SELECT * FROM deces;"
    log_message "Table deces bucketed and data populated."
else
    log_message "Failed to create and bucket the deces table."
fi