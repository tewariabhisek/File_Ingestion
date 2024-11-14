# Copy files from Unix System to DBFS Script

This script copies the event json file from a unix system to DBFS path to be processed by Spark.

---

## Getting Started

### Prerequisites
1. **Python Installation**: Ensure Python (>=3.6) is installed on your system.
2. **Install Required Packages**: Install dependencies using pip:
   
   ```bash
   pip install requests

3. Databricks Personal Access Token (PAT):

   ```bash
   Generate a PAT from your Databricks account and add it to the environment variables along with the databricks link. Go to User Settings > Access Tokens to create a new token.
   
### Running the Script
1. To copy the event file, run the script directly from the command line:

   ```bash
   python copy_files.py

### Scheduling the Script

1. **Open the crontab file in linux**:

   ```bash
   crontab -e

2. **Add a line to run the script every 2 hours for example**:

   ```bash
   0 */2 * * * /usr/bin/python3 /path/to/copy_files.py


