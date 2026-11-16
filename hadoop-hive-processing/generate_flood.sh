#!/bin/bash
# ============================================================
# FILE FLOOD GENERATOR
# ============================================================
# This script generates a large number of small CSV files
# to simulate a high-throughput data ingestion scenario.
#
# PURPOSE: Create the "processor overload" problem in NiFi
# where PutFile or PutHDFS can't keep up with the volume
# of incoming files, causing failures or dropped data.
#
# USAGE:
#   ./generate_flood.sh [number_of_files] [target_directory]
#
# DEFAULTS:
#   number_of_files = 500
#   target_directory = ./source
# ============================================================

NUM_FILES=${1:-500}
TARGET_DIR=${2:-./source}

echo "============================================"
echo "  NiFi File Flood Generator"
echo "============================================"
echo "  Files to generate : $NUM_FILES"
echo "  Target directory  : $TARGET_DIR"
echo "============================================"
echo ""

# Create target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

# CSV Header
HEADER="patient_id,name,age,admission_date,diagnosis,department,severity"

# Sample data arrays
NAMES=("Ahmed_Hassan" "Sara_Mohamed" "Omar_Ali" "Fatma_Ibrahim" "Youssef_Khalil" "Nour_Salem" "Mona_Adel" "Khaled_Mostafa" "Dina_Gamal" "Tarek_Samy")
DIAGNOSES=("Diabetes" "Hypertension" "Pneumonia" "Fracture" "Cardiac_Arrest" "Asthma" "Appendicitis" "Migraine" "Anemia" "Influenza")
DEPARTMENTS=("Cardiology" "Neurology" "Orthopedics" "Emergency" "Internal_Medicine" "Pediatrics" "Surgery" "ICU" "Oncology" "Radiology")
SEVERITIES=("Low" "Medium" "High" "Critical")

# Progress tracking
BATCH_SIZE=50
COUNT=0

echo "[$(date '+%H:%M:%S')] Starting file generation..."
echo ""

for i in $(seq 1 $NUM_FILES); do
    # Generate random data
    PATIENT_ID=$((RANDOM % 90000 + 10000))
    NAME=${NAMES[$((RANDOM % ${#NAMES[@]}))]}
    AGE=$((RANDOM % 70 + 18))
    
    # Random date in 2026
    MONTH=$(printf "%02d" $((RANDOM % 12 + 1)))
    DAY=$(printf "%02d" $((RANDOM % 28 + 1)))
    ADMISSION_DATE="2026-${MONTH}-${DAY}"
    
    DIAGNOSIS=${DIAGNOSES[$((RANDOM % ${#DIAGNOSES[@]}))]}
    DEPARTMENT=${DEPARTMENTS[$((RANDOM % ${#DEPARTMENTS[@]}))]}
    SEVERITY=${SEVERITIES[$((RANDOM % ${#SEVERITIES[@]}))]}
    
    # Generate between 1-20 rows per file (varying file sizes)
    NUM_ROWS=$((RANDOM % 20 + 1))
    
    FILENAME="patient_batch_${i}_$(date +%s%N).csv"
    
    # Write the CSV file
    echo "$HEADER" > "${TARGET_DIR}/${FILENAME}"
    
    for j in $(seq 1 $NUM_ROWS); do
        PATIENT_ID=$((RANDOM % 90000 + 10000))
        NAME=${NAMES[$((RANDOM % ${#NAMES[@]}))]}
        AGE=$((RANDOM % 70 + 18))
        MONTH=$(printf "%02d" $((RANDOM % 12 + 1)))
        DAY=$(printf "%02d" $((RANDOM % 28 + 1)))
        ADMISSION_DATE="2026-${MONTH}-${DAY}"
        DIAGNOSIS=${DIAGNOSES[$((RANDOM % ${#DIAGNOSES[@]}))]}
        DEPARTMENT=${DEPARTMENTS[$((RANDOM % ${#DEPARTMENTS[@]}))]}
        SEVERITY=${SEVERITIES[$((RANDOM % ${#SEVERITIES[@]}))]}
        
        echo "${PATIENT_ID},${NAME},${AGE},${ADMISSION_DATE},${DIAGNOSIS},${DEPARTMENT},${SEVERITY}" >> "${TARGET_DIR}/${FILENAME}"
    done
    
    COUNT=$((COUNT + 1))
    
    # Show progress every BATCH_SIZE files
    if [ $((COUNT % BATCH_SIZE)) -eq 0 ]; then
        echo "[$(date '+%H:%M:%S')] Generated $COUNT / $NUM_FILES files..."
    fi
done

echo ""
echo "============================================"
echo "  GENERATION COMPLETE"
echo "============================================"
echo "  Total files created : $COUNT"
echo "  Location            : $TARGET_DIR"
echo "  Total size          : $(du -sh $TARGET_DIR | cut -f1)"
echo "============================================"
echo ""
echo "Now go to NiFi (https://localhost:8443/nifi) and"
echo "watch how the processors handle the flood!"
echo ""
