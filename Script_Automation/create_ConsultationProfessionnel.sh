#!/bin/bash
# create_ConsultationProfessionnel.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'ConsultationProfessionnel';")

if [[ $table_exists == *"ConsultationProfessionnel"* ]]; then
    log_message "Table ConsultationProfessionnel already exists ..."
    hive -e "USE healthcare; DROP TABLE ConsultationProfessionnel;"
    log_message "Table ConsultationProfessionnel dropped successfully."
fi

log_message "Table ConsultationProfessionnel does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS ConsultationProfessionnel (
    Specialite STRING,
    nb_consultation INT
)
CLUSTERED BY (Specialite) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data/ConsultationProfessionnel';
"

if hive -e "$hive_query"; then
    log_message "Table ConsultationProfessionnel created and bucketed successfully.
    "
    # Peupler les buckets
    hive -e "USE healthcare; INSERT OVERWRITE TABLE ConsultationProfessionnel SELECT * FROM ConsultationProfessionnel;"
    log_message "Table ConsultationProfessionnel bucketed and data populated."
else
    log_message "Failed to create and bucket the ConsultationProfessionnel table."
fi