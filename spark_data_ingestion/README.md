# Synthetic Event Generation Script

This script ingests event data from files, processes and cleans the data (including removing duplicates), and saves it in partitioned Parquet format. The data is organized by event date and type for efficient storage and retrieval.

---

## Getting Started

### Prerequisites
1. **Databricks Workspace**: Access to a Databricks workspace with permission to create jobs and clusters.
2. **Personal Access Token (PAT)**: A Databricks PAT for authentication.
3. **Databricks CLI (optional)**: Set up the Databricks CLI to manage files and jobs more easily.

### Deployment and Running the Script
**Step 1: Upload Script to Databricks**
  1. Upload file_ingestion.py to DBFS (Databricks File System):

     ```bash
     databricks fs cp file_ingestion.py dbfs:/FileStore/scripts/file_ingestion.py
     
**Step 2: Configure the Production Job**
  1. Create a New Job:

     ```bash
     Go to Workflows > Jobs in Databricks and click Create Job.
  2. Define Job Parameters:

     ```bash
     Name: Assign a meaningful name, such as Event Ingestion Job.
     Task Type: Select Python Script.
     Path: Enter the DBFS path to the script, e.g., dbfs:/FileStore/scripts/file_ingestion.py.

  3. Set Up the Cluster:

     ```bash
     Cluster Type: Choose New Job Cluster or an existing high-concurrency cluster for production.
     Cluster Configuration:
      Set the node type, worker count, and autoscaling as required.
      Enable autoscaling to optimize resource use.
      Set a termination policy to shut down idle clusters when the job completes.
     
  4. Schedule the Job (Optional):

     ```bash
     Configure a schedule if you want the job to run periodically.
     For one-time runs, skip scheduling.

  5. Set Notifications and Alerts:

     ```bash
     Set up email or webhook notifications for job start, success, or failure to monitor job status.
