#!/bin/bash

# Chargement des configurations et des fonctions depuis initialisation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Fonction pour surveiller et traiter les nouveaux fichiers
watch_and_process() {
    while true; do
        # Détecter les fichiers .txt dans le dossier de données HDFS
        new_files=$(hdfs dfs -ls $LOCAL_DATA_PATH | grep 'txt' | awk '{print $NF}')
        has_files=false

        for file in $new_files; do
            # Extraire uniquement le nom de fichier pour la vérification
            filename=$(basename "$file")
            if [[ "$filename" =~ ^(ConsultationPatient_Diagnostic_Temps.txt|ConsultationPatient_Etablissement_Temps.txt|ConsultationProfessionnel.txt|Deces.txt|HospitalisationAge.txt|HospitalisationPatient.txt|HospitalisationPatient_Diagnostic.txt|HospitalisationSexe.txt|SatisfactionRegion.txt)$ ]]; then
                has_files=true
                break
            fi
        done

        if [[ "$has_files" == true ]]; then
            log_message "New files detected, starting processing..."

            # Exécuter les fonctions nécessaires de initialisation.sh            
            move_txt_files
            setup_dir
            setup_hdfs
            setup_hive_config
            create_healthcare_db

            for script in "${scripts[@]}"; do
                log_message "Updating $script for new data..."
                bash "$LOCAL_SCRIPT_PATH/$script"
            done

            bash "$LOCAL_SCRIPT_PATH/verify_tables.sh"
            log_message "Verification process executed after updating tables."
        else
            log_message "No new files detected."
        fi

        sleep 300
    done
}

# Définition des scripts pour la création/mise à jour des tables
declare -a scripts=(
    "create_ConsultationPatient_Diagnostic_Temps.sh"
    "create_ConsultationPatient_Etablissement_Temps.sh"
    "create_ConsultationProfessionnel.sh"
    "create_Deces.sh"
    "create_HospitalisationAge.sh"
    "create_HospitalisationPatient.sh"
    "create_HospitalisationPatient_Diagnostic.sh"
    "create_HospitalisationSexe.sh"
    "create_SatisfactionRegion.sh"  
)

# Appeler la fonction de surveillance et de traitement
watch_and_process