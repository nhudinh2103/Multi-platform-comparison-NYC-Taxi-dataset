#!/bin/bash

# sync_all_notebooks.sh - Sync all notebooks to Databricks workspace
# This script syncs all notebooks in the Workspace/NYCTaxi/notebooks directory to Databricks

set -e  # Exit immediately if a command exits with a non-zero status

# Define source and destination directories
SOURCE_DIR="Workspace/NYCTaxi/jupyter-notebook"
DEST_DIR="/NYCTaxi/jupyter-notebook"

# Check if the source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# Create a function to sync a notebook
sync_notebook() {
    local notebook_path="$1"
    local relative_path="${notebook_path#$SOURCE_DIR/}"
    local dest_path="$DEST_DIR/$relative_path"
    
    # Remove the .ipynb extension for the destination path
    dest_path="${dest_path%.ipynb}"
    
    echo "Syncing $notebook_path to $dest_path"
    "$(dirname "$0")/sync_notebook.sh" "$notebook_path" "$dest_path"
}

# Find all notebooks and sync them
echo "Finding notebooks in $SOURCE_DIR..."
find "$SOURCE_DIR" -name "*.ipynb" | while read notebook_path; do
    sync_notebook "$notebook_path"
done

echo "All notebooks synced to Databricks workspace."
