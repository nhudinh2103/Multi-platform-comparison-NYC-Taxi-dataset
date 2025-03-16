#!/bin/bash

# sync_notebook.sh - Sync a local notebook to Databricks workspace
# Usage: ./sync_notebook.sh <local_notebook_path> <remote_path>
# Example: ./sync_notebook.sh src/dual_mode_notebook.ipynb /Users/me/dual_mode_notebook

set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Print commands and their arguments as they are executed

# Check if required arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: ./sync_notebook.sh <local_notebook_path> <remote_path>"
    echo "Example: ./sync_notebook.sh src/dual_mode_notebook.ipynb /Users/me/dual_mode_notebook"
    exit 1
fi

LOCAL_PATH="$1"
REMOTE_PATH="$2"

# Check if the local file exists
if [ ! -f "$LOCAL_PATH" ]; then
    echo "Error: Local file '$LOCAL_PATH' does not exist."
    exit 1
fi

echo "Reading Databricks configuration..."

# Get Databricks token from CLI config
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

echo "Syncing notebook from '$LOCAL_PATH' to '$REMOTE_PATH' on $WORKSPACE_URL"

# Normalize the remote path
# If the path doesn't start with /, add it
if [[ ! "$REMOTE_PATH" =~ ^/ ]]; then
    REMOTE_PATH="/$REMOTE_PATH"
    echo "Added leading slash to path: $REMOTE_PATH"
fi

# If the path starts with /Workspace, remove it as the API doesn't expect it
if [[ "$REMOTE_PATH" =~ ^/Workspace ]]; then
    REMOTE_PATH="${REMOTE_PATH:10}"
    echo "Removed /Workspace prefix from path: $REMOTE_PATH"
fi

# Convert the notebook to Databricks format
echo "Converting notebook to Databricks format..."
CONVERTED_FILE=$(mktemp)
python convert_notebook.py "$LOCAL_PATH" "$CONVERTED_FILE"

# Get base64 content of the converted notebook
NOTEBOOK_CONTENT=$(base64 -w 0 "$CONVERTED_FILE")
echo "Created base64 content of the converted notebook"

# Create JSON payload with proper commas
TEMP_FILE=$(mktemp)
cat > "$TEMP_FILE" << EOF
{
  "path": "$REMOTE_PATH",
  "format": "SOURCE",
  "language": "PYTHON",
  "overwrite": true,
  "content": "$NOTEBOOK_CONTENT"
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

# Import the notebook to Databricks workspace
echo "Importing notebook to Databricks workspace..."
RESPONSE=$(curl -v -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d @"$TEMP_FILE" \
    "$WORKSPACE_URL/api/2.0/workspace/import" 2>&1)

echo "API Response: $RESPONSE"

# Clean up temporary files
rm -f "$TEMP_FILE" "$CONVERTED_FILE"

# Check if the import was successful
if [[ "$RESPONSE" == *"error"* ]]; then
    echo "Error syncing notebook: $RESPONSE"
    exit 1
else
    echo "Notebook successfully synced to $REMOTE_PATH"
    echo "You can now run it remotely using:"
    echo "python run_remote_notebook.py $REMOTE_PATH"
    echo ""
    echo "Or open it in the Databricks workspace at:"
    echo "$WORKSPACE_URL/#workspace$REMOTE_PATH"
fi
