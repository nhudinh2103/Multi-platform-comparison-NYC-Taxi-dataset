#!/bin/bash

# sync_sql.sh - Sync SQL files from a local folder to Databricks workspace
# Usage: ./sync_sql.sh <source_folder> <remote_folder>
# Example: ./sync_sql.sh Workspace/CarsProject/sql/transform /Users/me/sql/transform

set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Print commands and their arguments as they are executed

# Check if required arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: ./sync_sql.sh <source_folder> <remote_folder>"
    echo "Example: ./sync_sql.sh Workspace/CarsProject/sql/transform /Users/me/sql/transform"
    exit 1
fi

SOURCE_FOLDER="$1"
REMOTE_FOLDER="$2"

# Check if the source folder exists
if [ ! -d "$SOURCE_FOLDER" ]; then
    echo "Error: Source folder '$SOURCE_FOLDER' does not exist."
    exit 1
fi

echo "Reading Databricks configuration..."

# Get Databricks token and host from CLI config
if [ -f ~/.databrickscfg ]; then
    echo "Found ~/.databrickscfg file"
    # Try to get token with or without the bracket
    TOKEN=$(grep -A2 "DEFAULT]" ~/.databrickscfg | grep "token" | cut -d'=' -f2 | tr -d ' ')
    if [ -z "$TOKEN" ]; then
        # Try alternative format
        TOKEN=$(grep "token" ~/.databrickscfg | cut -d'=' -f2 | tr -d ' ')
    fi
    
    # Get the host (workspace URL)
    WORKSPACE_URL=$(grep -A2 "DEFAULT]" ~/.databrickscfg | grep "host" | cut -d'=' -f2 | tr -d ' ')
    if [ -z "$WORKSPACE_URL" ]; then
        # Try alternative format
        WORKSPACE_URL=$(grep "host" ~/.databrickscfg | cut -d'=' -f2 | tr -d ' ')
    fi
    
    if [ -z "$TOKEN" ]; then
        echo "Error: Could not find token in ~/.databrickscfg"
        echo "Please run 'databricks configure --token' to set up your credentials."
        exit 1
    fi
    
    if [ -z "$WORKSPACE_URL" ]; then
        echo "Error: Could not find host in ~/.databrickscfg"
        echo "Please run 'databricks configure --token' to set up your credentials."
        exit 1
    fi
    
    echo "Successfully retrieved token and workspace URL from ~/.databrickscfg"
    echo "Using workspace URL: $WORKSPACE_URL"
else
    echo "Error: ~/.databrickscfg file not found"
    echo "Please run 'databricks configure --token' to set up your credentials."
    exit 1
fi

# Normalize the remote folder path
# If the path doesn't start with /, add it
if [[ ! "$REMOTE_FOLDER" =~ ^/ ]]; then
    REMOTE_FOLDER="/$REMOTE_FOLDER"
    echo "Added leading slash to path: $REMOTE_FOLDER"
fi

# If the path starts with /Workspace, remove it as the API doesn't expect it
if [[ "$REMOTE_FOLDER" =~ ^/Workspace ]]; then
    REMOTE_FOLDER="${REMOTE_FOLDER:10}"
    echo "Removed /Workspace prefix from path: $REMOTE_FOLDER"
fi

# Find all SQL files in the source folder
SQL_FILES=$(find "$SOURCE_FOLDER" -name "*.sql" -type f)
if [ -z "$SQL_FILES" ]; then
    echo "No SQL files found in '$SOURCE_FOLDER'."
    exit 1
fi

# Process each SQL file
for SQL_FILE in $SQL_FILES; do
    echo "Processing SQL file: $SQL_FILE"
    
    # Get the relative path from the source folder
    REL_PATH="${SQL_FILE#$SOURCE_FOLDER/}"
    
    # Construct the remote path
    REMOTE_PATH="$REMOTE_FOLDER/$REL_PATH"
    
    # Remove file extension for the Databricks path (optional)
    REMOTE_PATH_NO_EXT="${REMOTE_PATH%.sql}"
    
    echo "Syncing SQL file from '$SQL_FILE' to '$REMOTE_PATH_NO_EXT' on $WORKSPACE_URL"
    
    # Get base64 content of the SQL file
    SQL_CONTENT=$(base64 -w 0 "$SQL_FILE")
    echo "Created base64 content of the SQL file"
    
    # Create JSON payload
    TEMP_FILE=$(mktemp)
    cat > "$TEMP_FILE" << EOF
{
  "path": "$REMOTE_PATH_NO_EXT",
  "format": "SOURCE",
  "language": "SQL",
  "overwrite": true,
  "content": "$SQL_CONTENT"
}
EOF
    echo "Created JSON payload for API request and saved to temporary file"
    
    # Output the curl command for debugging
    echo "Curl command for debugging:"
    echo "curl -v -X POST \\"
    echo "    -H \"Authorization: Bearer $TOKEN\" \\"
    echo "    -H \"Content-Type: application/json\" \\"
    echo "    -d @\"$TEMP_FILE\" \\"
    echo "    \"$WORKSPACE_URL/api/2.0/workspace/import\""
    
    # Import the SQL file to Databricks workspace
    echo "Importing SQL file to Databricks workspace..."
    RESPONSE=$(curl -v -X POST \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d @"$TEMP_FILE" \
        "$WORKSPACE_URL/api/2.0/workspace/import" 2>&1)
    
    echo "API Response: $RESPONSE"
    
    # Clean up temporary file
    rm -f "$TEMP_FILE"
    
    # Check if the import was successful
    if [[ "$RESPONSE" == *"error"* ]]; then
        echo "Error syncing SQL file: $RESPONSE"
        # Continue with other files instead of exiting
        continue
    else
        echo "SQL file successfully synced to $REMOTE_PATH_NO_EXT"
        echo "You can now open it in the Databricks workspace at:"
        echo "$WORKSPACE_URL/#workspace$REMOTE_PATH_NO_EXT"
    fi
done

echo "SQL file synchronization complete!"
