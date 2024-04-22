#!/bin/bash

# Chargement des configurations et des fonctions depuis initialisation.sh
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Exécuter les fonctions de configuration initiale si ce n'est déjà fait dans initialization.sh
setup_dir
setup_hdfs
setup_hive_config
create_healthcare_db

# Fonction pour surveiller et traiter les nouveaux fichiers
watch_and_process() {
    # Boucle infinie pour surveiller en continu les nouveaux fichiers
    while true; do
        # Détecter les fichiers .txt dans le dossier de données local
        new_files=$(find $LOCAL_DATA_PATH -type f -name '*.txt')

        if [[ ! -z "$new_files" ]]; then
            log_message "New files detected, starting processing..."

            # Déplacer les nouveaux fichiers vers HDFS
            move_txt_files

            # Exécuter chaque script de création/mise à jour de tables
            for script in "${scripts[@]}"; do
                log_message "Updating $script for new data..."
                bash "$LOCAL_SCRIPT_PATH/$script"
            done

            # Exécuter le script de vérification après la mise à jour des tables
            bash "$LOCAL_SCRIPT_PATH/verify_tables.sh"
            log_message "Verification process executed after updating tables."

            # Suppression des fichiers locaux après traitement pour éviter le retraitement
            for file in $new_files; do
                rm "$file"
            done
        else
            log_message "No new files detected."
        fi

        # Attendre avant de vérifier à nouveau
        sleep 300  # Temps d'attente en secondes (ici 5 minutes)
    done
}


# Définition des scripts pour la création/mise à jour des tables
declare -a scripts=(
    "create_ConsultationPatient_Diagnostic_Temps.sh",
    "create_ConsultationPatient_Etablissement_Temps.sh",
    "create_ConsultationProfessionnel.sh",
    "create_Deces.sh",
    "create_HospitalisationAge.sh",
    "create_HospitalisationPatient.sh",
    "create_HospitalisationPatient_Diagnostic.sh",
    "create_HospitalisationSexe.sh"
)

# Appeler la fonction de surveillance et de traitement
watch_and_process