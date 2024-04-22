#!/bin/bash
# create_Deces.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'Deces';")

if [[ $table_exists == *"Deces"* ]]; then
    log_message "Table Deces already exists ..."
    hive -e "USE healthcare; DROP TABLE Deces;"
    log_message "Table Deces dropped successfully."
fi

log_message "Table Deces does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS Deces (
    Region STRING,
    nb_deces INT
)
CLUSTERED BY (Region) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/Deces';
"

if hive -e "$hive_query"; then
    log_message "Table Deces created and bucketed successfully."
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE Deces SELECT * FROM Deces;"
    log_message "Table Deces bucketed and data populated."
else
    log_message "Failed to create and bucket the Deces table."
fi