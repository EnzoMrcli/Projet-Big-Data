#!/bin/bash
# verify_tables.sh

# Chargement des configurations et des fonctions depuis initialisation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Fonction pour vérifier les données et la structure de la table
verify_table() {
    local table_name=$1
    log_message "Starting verification for table: $table_name"

    # Vérifiez si la table existe
    local table_exists=$(hive -e "USE healthcare; SHOW TABLES LIKE '$table_name';")
    if [[ -z "$table_exists" ]]; then
        log_message "Verification failed: Table $table_name does not exist."
        return
    fi

    # Vérifier la présence de données dans la table
    local has_data=$(hive -e "USE healthcare; SELECT 1 FROM $table_name LIMIT 1;")
    if [[ -n "$has_data" ]]; then
        log_message "Data verification successful for $table_name. Data is present."
    else
        log_message "Data verification warning for $table_name: No data found!"
    fi

    # Vérifiez les partitions si la table est partitionnée
    if [[ "$table_name" == "ConsultationPatient_Diagnostic_Temps" || 
          "$table_name" == "ConsultationPatient_Etablissement_Temps" ||
          "$table_name" == "HospitalisationPatient_Diagnostic" ||
          "$table_name" == "SatisfactionRegion" ]]; then  
        local partitions=$(hive -e "USE healthcare; SHOW PARTITIONS $table_name;")
        log_message "Partitions in $table_name: $partitions"
    fi
}

# Liste des tables à vérifier avec les nouveaux noms adaptés aux fichiers de données
tables=(
    "ConsultationPatient_Diagnostic_Temps",
    "ConsultationPatient_Etablissement_Temps",
    "ConsultationProfessionnel",
    "Deces",
    "HospitalisationAge",
    "HospitalisationPatient",
    "HospitalisationPatient_Diagnostic",
    "HospitalisationSexe",
    "SatisfactionRegion"  
)


# Parcourez les tables et effectuez une vérification
for table in "${tables[@]}"; do
    verify_table "$table"
done

log_message "Verification completed for all tables."