#!/bin/bash

set -e 

LOG_FILE="vm_update.log"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] $1" | tee -a "$LOG_FILE"
}

log "Pulling the latest changes from the repository..."
git pull || { log "Failed to pull changes from GitHub!"; exit 1; }

if [ ! -d ".venv" ]; then
  log "Virtual environment not found. Creating one..."
  python3 -m venv .venv || { log "Failed to create virtual environment!"; exit 1; }
else
  log "Virtual environment already exists."
fi

log "Activating the virtual environment..."
source .venv/bin/activate || { log "Failed to activate virtual environment!"; exit 1; }

log "Installing dependencies from requirements.txt..."
pip install --quiet -r requirements.txt || { log "Failed to install dependencies!"; exit 1; }

log "Running the pipeline script..."
export PYTHONPATH="~/ElectricityCarbonMonitoring:$PYTHONPATH"
python3 scripts/pipeline.py || { log "Pipeline script execution failed!"; exit 1; }

log "Staging changes to GitHub..."
git add . || { log "Failed to stage changes!"; exit 1; }

log "Committing changes..."
git commit -m "daily update" || { log "Failed to commit changes!"; exit 1; }

log "Pushing changes to the repository..."
git push || { log "Failed to push changes to GitHub!"; exit 1; }

log "Update process completed successfully!"