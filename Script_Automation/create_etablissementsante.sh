#!/bin/bash
# create_etablissementsante.sh
LOG_PATH="/home/cloudera/script_automatisation/logs/logs.txt"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_PATH"
}

# Vérification de l'existence de la table
table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE 'etablissementsante';")

if [[ $table_exists == *"etablissementsante"* ]]; then
    log_message "Table etablissementsante already exists ..."
    hive -e "USE healthcare; DROP TABLE etablissementsante;"
    log_message "Table etablissementsante dropped successfully."
fi

log_message "Table etablissementsante does not exist, creating table..."
hive_query="USE healthcare; CREATE EXTERNAL TABLE IF NOT EXISTS etablissementsante (
    identifiant_organisation INT,
    adresse STRING,
    cedex STRING,
    code_commune STRING,
    code_postal INT,
    commune STRING,
    complement_destinataire STRING,
    complement_point_geographique STRING,
    email STRING,
    enseigne_commerciale_site STRING,
    finess_etablissement_juridique STRING,
    finess_site STRING,
    indice_repetition_voie STRING,
    mention_distribution STRING,
    numero_voie STRING,
    raison_sociale_site STRING,
    siren_site STRING,
    siret_site STRING,
    telecopie STRING,
    telephone STRING,
    telephone_2 STRING,
    type_voie STRING,
    voie STRING
)
PARTITIONED BY (pays STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/hive/data';"
if hive -e "$hive_query"; then
    log_message "Table etablissementsante created successfully."
else
    log_message "Failed to create the etablissementsante table."
fi