#!/bin/bash

# Exit immediately if any command fails
set -e 

# Log file for the script
LOG_FILE="vm_update.log"

# Function to log messages
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] $1" | tee -a "$LOG_FILE"
}

# 1. Pull the latest changes from GitHub
log "Pulling the latest changes from the repository..."
git pull || { log "Failed to pull changes from GitHub!"; exit 1; }

# 2. Check if the virtual environment exists, create it if not
if [ ! -d ".venv" ]; then
  log "Virtual environment not found. Creating one..."
  python -m venv .venv || { log "Failed to create virtual environment!"; exit 1; }
else
  log "Virtual environment already exists."
fi

# 3. Activate the virtual environment
log "Activating the virtual environment..."
source .venv/bin/activate || { log "Failed to activate virtual environment!"; exit 1; }

# 4. Install dependencies
log "Installing dependencies from requirements.txt..."
pip install --quiet -r requirements.txt || { log "Failed to install dependencies!"; exit 1; }

# 5. Run the pipeline script
log "Running the pipeline script..."
python scripts/pipeline.py || { log "Pipeline script execution failed!"; exit 1; }

# 6. Stage and commit changes
log "Staging changes to GitHub..."
git add . || { log "Failed to stage changes!"; exit 1; }

log "Committing changes..."
git commit -m "daily update" || { log "Failed to commit changes!"; exit 1; }

# 7. Push changes to the repository
log "Pushing changes to the repository..."
git push || { log "Failed to push changes to GitHub!"; exit 1; }

log "Update process completed successfully!"