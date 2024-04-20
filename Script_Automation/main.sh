#!/bin/bash

# Chemins d'accès aux scripts et logs
source /home/cloudera/script_automatisation/scripts/initialisation.sh

# Fonction pour loguer les messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOCAL_LOG_PATH/main.log"
}

# Liste des scripts pour la création des tables
declare -a scripts=(
    "create_consultation.sh",
    "create_deces.sh",
    "create_diagnostic.sh",
    "create_etablissementsante.sh",
    "create_hospitalisation.sh",
    "create_patient.sh",
    "create_professionnelsante.sh",
    "create_tempsconsultation.sh",
    "create_tempshospitalisation.sh"
)

# Exécution initiale de tous les scripts pour créer ou mettre à jour les tables
for script in "${scripts[@]}"; do
    log_message "Executing $script..."
    bash "$LOCAL_SCRIPT_PATH/$script"
done
log_message "Initial setup completed."

# Boucle infinie pour la surveillance des nouveaux fichiers
while true; do
    # Vérifiez si de nouveaux fichiers .txt ont été ajoutés localement
    new_files=$(find $LOCAL_DATA_PATH -type f -name '*.txt')
    if [[ ! -z "$new_files" ]]; then
        log_message "New files detected, moving files..."
        # Appel de la fonction pour déplacer les fichiers
        move_txt_files
        log_message "New files moved to HDFS, starting processing..."
        
        # Exécuter chaque script de mise à jour
        for script in "${scripts[@]}"; do
            log_message "Updating $script for new data..."
            bash "$LOCAL_SCRIPT_PATH/$script"
        done
        log_message "Data processing completed."
    else
        log_message "No new files detected."
    fi
    
    # Attendre 5 minutes avant la prochaine vérification
    sleep 300
done

log_message "All updates have been processed."