# Activate the virtual environment
source /home/user1/startups/crunch_base/env/bin/python
pkill -f python
# Navigate to the script directory
cd ~/startups/crunch_base

# Create log directory if it doesn't exist
mkdir -p log
log_file="log/main.log"
pwd
# Run the scraper
/home/user1/startups/crunch_base/env/bin/python main.py
