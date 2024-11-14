import requests

# Constants
DATABRICKS_INSTANCE = "https://community.cloud.databricks.com"  # Replace with your Databricks URL
TOKEN = "your_personal_access_token"  # Replace with your Personal Access Token
LOCAL_FILE_PATH = "/path/to/your/largefile.txt"  # Replace with your local file path
DBFS_DESTINATION_PATH = "/FileStore/largefile.txt"  # DBFS path where the file will be saved
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB

def upload_large_file_to_dbfs(local_file_path, dbfs_path):
    # Step 1: Initialize the file upload with dbfs/create
    create_url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/create"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    create_response = requests.post(create_url, headers=headers, json={
        "path": dbfs_path,
        "overwrite": True
    })

    if create_response.status_code != 200:
        print(f"Failed to initialize upload: {create_response.json()}")
        return
    
    # Get the file handle from the response
    handle = create_response.json()["handle"]
    print(f"Upload initialized with handle: {handle}")

    # Step 2: Read the file and upload in chunks with dbfs/add-block
    with open(local_file_path, "rb") as f:
        while True:
            # Read a chunk of the file
            data = f.read(CHUNK_SIZE)
            if not data:
                break  # End of file

            # Add this chunk to DBFS
            add_block_url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/add-block"
            add_block_response = requests.post(add_block_url, headers=headers, json={
                "handle": handle,
                "data": data.decode("latin1")  # Encode binary data to latin1 for JSON transfer
            })

            if add_block_response.status_code != 200:
                print(f"Failed to upload chunk: {add_block_response.json()}")
                return

            print(f"Uploaded chunk of size: {len(data)} bytes")

    # Step 3: Finalize the file upload with dbfs/close
    close_url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/close"
    close_response = requests.post(close_url, headers=headers, json={"handle": handle})

    if close_response.status_code == 200:
        print(f"File successfully uploaded to {dbfs_path}")
    else:
        print(f"Failed to finalize upload: {close_response.json()}")

# Run the upload function

if __name__ == "__main__":
  upload_large_file_to_dbfs(LOCAL_FILE_PATH, DBFS_DESTINATION_PATH)

