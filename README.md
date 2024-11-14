# File_Ingestion

![image](https://github.com/user-attachments/assets/a4396bbd-5ef0-45bc-b97c-8fa120b2aca2)


The image depicts a data processing workflow that starts from a Unix file system and involves Databricks for data transformation. Here's a breakdown of each step in the flow:
1.	**Unix File System**: The process begins with data stored in a Unix file system.
2.	**Python Script for Ingestion**: A Python script is used to read the file from the Unix file system and ingest it into a Databricks file system (DBFS) path. This script pulls the data from its source location and transfers it into the DBFS for further processing.
3.	**Databricks File System (DBFS)**: The data, now in DBFS, serves as an intermediary storage in the Databricks environment. This allows Databricks to access the file for subsequent transformations.
4.	**Spark Job on Databricks**: A Spark job running on Databricks processes the ingested data. This job performs transformations or computations on the data as per the specified requirements.
5.	**Transformed Data in Parquet Format**: The final output of the Spark job is transformed data, saved in Parquet format, within the Databricks file system.
