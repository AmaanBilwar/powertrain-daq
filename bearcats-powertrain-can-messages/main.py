from flask import Flask, request, jsonify
from flask_cors import CORS
import can
import time
import threading
import json
import os,uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def upload_to_azure(directory_path=None, container_name="can-messages"):
    """
    Upload all files from the specified directory to Azure Blob Storage.
    
    Args:
        directory_path: Path to the directory containing files to upload (default: current directory)
        container_name: Name of the Azure Blob Storage container
    """
    # Set default directory to current directory if not specified
    if directory_path is None:
        directory_path = os.getcwd()
    
    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)
    
    # Ensure container exists
    try:
        container_client.get_container_properties()
    except Exception:
        # Create container if it doesn't exist
        container_client.create_container()
        print(f"Container '{container_name}' created.")
    
    # Check if directory exists
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory '{directory_path}' not found")
    
    # Upload all files in the directory
    files_uploaded = 0
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Skip directories
        if os.path.isdir(file_path):
            continue
            
        # Create a blob client for each file
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=filename
        )
        
        print(f"Uploading {filename} to Azure Storage as blob")
        
        # Upload the file
        with open(file=file_path, mode="rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        files_uploaded += 1
    
    print(f"Upload complete. {files_uploaded} files uploaded to container '{container_name}'")


def upload_to_cosmos(data_directory=None, database_name="can-database", container_name="can-messages"):
    """
    Upload JSON files from the specified directory to Azure Cosmos DB using batch operations.
    
    Args:
        data_directory: Path to the directory containing JSON files to upload (default: current directory)
        database_name: Name of the Cosmos DB database
        container_name: Name of the Cosmos DB container
    """
    # Import required libraries for Cosmos DB
    from azure.cosmos import CosmosClient, exceptions, PartitionKey
    import time
    
    # Set default directory to current directory if not specified
    if data_directory is None:
        data_directory = os.getcwd()
    
    # Check if directory exists
    if not os.path.exists(data_directory):
        raise FileNotFoundError(f"Directory '{data_directory}' not found")
    
    # Get Cosmos DB connection string from environment variables
    cosmos_connection_string = os.getenv('COSMOS_CONNECTION_STRING')
    if not cosmos_connection_string:
        raise ValueError("COSMOS_CONNECTION_STRING environment variable is not set")
    
    start_time = time.time()
    print("Starting Cosmos DB upload...")
    
    # Initialize Cosmos client
    client = CosmosClient.from_connection_string(cosmos_connection_string)
    
    # Create database if it doesn't exist
    try:
        database = client.get_database_client(database_name)
        database.read()
    except exceptions.CosmosResourceNotFoundError:
        print(f"Creating database: {database_name}")
        database = client.create_database(database_name)
    
    # Create container if it doesn't exist
    try:
        container = database.get_container_client(container_name)
        container.read()
    except exceptions.CosmosResourceNotFoundError:
        print(f"Creating container: {container_name}")
        container = database.create_container(
            id=container_name,
            partition_key=PartitionKey(path="/id"),
        )
    
    # Upload JSON files in the directory
    items_uploaded = 0
    batch_size = 100  # Adjust based on your document size
    
    for filename in os.listdir(data_directory):
        file_path = os.path.join(data_directory, filename)
        
        # Skip directories and non-JSON files
        if os.path.isdir(file_path) or not filename.endswith('.json'):
            continue
        
        print(f"Processing {filename} for Cosmos DB upload")
        
        try:
            # Read JSON file
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # If data is not a list, convert it to a list
            if not isinstance(data, list):
                data = [data]
            
            # Ensure each document has an id
            for item in data:
                if 'id' not in item:
                    item['id'] = str(uuid.uuid4())
            
            # Upload in batches
            total_items = len(data)
            for i in range(0, total_items, batch_size):
                batch = data[i:i+batch_size]
                
                # Use individual create operations for each item in the batch
                for item in batch:
                    container.upsert_item(body=item)
                    items_uploaded += 1
                
                # Print progress for large files
                if total_items > 1000:
                    print(f"  Uploaded {min(i+batch_size, total_items)}/{total_items} items")
                
        except json.JSONDecodeError:
            print(f"Error: {filename} is not a valid JSON file. Skipping.")
        except Exception as e:
            print(f"Error uploading {filename}: {str(e)}")
    
    elapsed_time = time.time() - start_time
    print(f"Upload complete. {items_uploaded} items uploaded to Cosmos DB container '{container_name}' in {elapsed_time:.2f} seconds")


def optimized_upload_to_cosmos(data_directory=None, database_name="can-database", container_name="can-messages"):
    """
    Optimized version of upload_to_cosmos with parallel processing and better batching
    """
    from azure.cosmos import CosmosClient, exceptions, PartitionKey
    import time
    import concurrent.futures
    
    # Set default directory to current directory if not specified
    if data_directory is None:
        data_directory = os.getcwd()
    
    # Check if directory exists
    if not os.path.exists(data_directory):
        raise FileNotFoundError(f"Directory '{data_directory}' not found")
    
    # Get Cosmos DB connection string from environment variables
    cosmos_connection_string = os.getenv('COSMOS_CONNECTION_STRING')
    if not cosmos_connection_string:
        raise ValueError("COSMOS_CONNECTION_STRING environment variable is not set")
    
    start_time = time.time()
    print("Starting optimized Cosmos DB upload...")
    
    # Initialize Cosmos client
    client = CosmosClient.from_connection_string(cosmos_connection_string)
    
    # Create database if it doesn't exist
    try:
        database = client.get_database_client(database_name)
        database.read()
    except exceptions.CosmosResourceNotFoundError:
        print(f"Creating database: {database_name}")
        database = client.create_database(database_name)
    
    # Create container if it doesn't exist
    try:
        container = database.get_container_client(container_name)
        container.read()
    except exceptions.CosmosResourceNotFoundError:
        print(f"Creating container: {container_name}")
        container = database.create_container(
            id=container_name,
            partition_key=PartitionKey(path="/id"),
        )
    
    # Find all JSON files in the directory
    json_files = [os.path.join(data_directory, f) for f in os.listdir(data_directory) 
                 if os.path.isfile(os.path.join(data_directory, f)) and f.endswith('.json')]
    
    items_uploaded = 0
    batch_size = 100  # You may need to adjust this based on your document size
    
    # Function to process a single file
    def process_file(file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # If data is not a list, convert it to a list
            if not isinstance(data, list):
                data = [data]
            
            # Ensure each document has an id
            for item in data:
                if 'id' not in item:
                    item['id'] = str(uuid.uuid4())
            
            return data
        except json.JSONDecodeError:
            print(f"Error: {os.path.basename(file_path)} is not a valid JSON file. Skipping.")
            return []
        except Exception as e:
            print(f"Error processing {os.path.basename(file_path)}: {str(e)}")
            return []
    
    # Process files in parallel to prepare data
    all_items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_file = {executor.submit(process_file, file_path): file_path for file_path in json_files}
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                items = future.result()
                all_items.extend(items)
                print(f"Prepared {len(items)} items from {os.path.basename(file_path)}")
            except Exception as e:
                print(f"Exception processing {os.path.basename(file_path)}: {str(e)}")
    
    # Upload all items in optimized batches
    print(f"Uploading {len(all_items)} total items to Cosmos DB...")
    
    for i in range(0, len(all_items), batch_size):
        batch = all_items[i:i+batch_size]
        
        # Use bulk executor for batch processing
        operations = []
        for item in batch:
            operations.append(
                {"operationType": "Upsert", "resourceBody": item}
            )
        
        if operations:
            container.execute_bulk_operations(operations)
            items_uploaded += len(operations)
            
            # Print progress
            print(f"  Uploaded {min(i+batch_size, len(all_items))}/{len(all_items)} items")
    
    elapsed_time = time.time() - start_time
    print(f"Optimized upload complete. {items_uploaded} items uploaded to Cosmos DB container '{container_name}' in {elapsed_time:.2f} seconds")
    return items_uploaded


def create_json_from_csv(csv_file_path, output_dir):
    """
    Convert CSV data of decoded CAN messages to JSON files.
    
    Args:
        csv_file_path: Path to the CSV file containing decoded CAN messages
        output_dir: Directory to save the generated JSON files
    """
    import csv
    import json
    import os
    from datetime import datetime
    
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    try:
        with open(csv_file_path, 'r') as csv_file:
            # Skip the comment line if it exists
            first_line = csv_file.readline().strip()
            if first_line.startswith('//'):
                csv_reader = csv.DictReader(csv_file)
            else:
                # If no comment line, reset file pointer and read normally
                csv_file.seek(0)
                csv_reader = csv.DictReader(csv_file)
            
            # Group messages by timestamp for batch processing
            messages_by_timestamp = {}
            for row in csv_reader:
                timestamp = row.get('timestamp', '')
                message_id = row.get('message_id', '')
                decoded_data = row.get('decoded_data', '')
                
                # Skip if any required field is missing
                if not all([timestamp, message_id, decoded_data]):
                    continue
                
                # Parse decoded_data from string to dict
                try:
                    # Remove single quotes and convert to proper JSON format
                    decoded_data = decoded_data.replace("'", '"')
                    data_dict = json.loads(decoded_data)
                except json.JSONDecodeError:
                    print(f"Error parsing data: {decoded_data}")
                    continue
                
                # Create message object
                message = {
                    "timestamp": timestamp,
                    "message_id": message_id,
                    "data": data_dict
                }
                
                if timestamp not in messages_by_timestamp:
                    messages_by_timestamp[timestamp] = []
                
                messages_by_timestamp[timestamp].append(message)
            
            # Save each timestamp group as a separate JSON file
            files_created = 0
            for timestamp, messages in messages_by_timestamp.items():
                # Create a valid filename from the timestamp
                # Replace colons and other problematic characters
                filename = timestamp.replace(':', '-').replace('T', '_').replace('Z', '') + '.json'
                file_path = os.path.join(output_dir, filename)
                
                with open(file_path, 'w') as json_file:
                    json.dump(messages, json_file, indent=2)
                
                files_created += 1
                
            print(f"Successfully created {files_created} JSON files in {output_dir}")
            return files_created
    
    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        return 0


def process_can_data_and_upload(raw_can_file, dbc_files, output_dir="processed_can_data"):
    """
    Process raw CAN data using DBC files, create JSON files, and upload to Azure.
    
    Args:
        raw_can_file: Path to the raw CAN data file
        dbc_files: List of paths to DBC files
        output_dir: Directory to save processed JSON files
    """
    import cantools
    import json
    from datetime import datetime

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Load DBC files
    db = cantools.database.Database()
    for dbc_file in dbc_files:
        try:
            db.add_dbc_file(dbc_file)
        except Exception as e:
            print(f"Error loading DBC file {dbc_file}: {str(e)}")

    # Process CAN messages
    messages_by_timestamp = {}
    
    try:
        with open(raw_can_file, 'r') as f:
            # Skip header lines
            while True:
                line = f.readline()
                if line.startswith('Chn'):
                    break
            
            for line in f:
                # Skip empty lines, error frames and 'Logging stopped'
                if not line.strip() or "ErrorFrame" in line or "Logging stopped" in line:
                    continue
                
                # Parse CAN message
                parts = line.strip().split()
                if len(parts) < 10:  # Basic validation
                    continue
                
                try:
                    timestamp = float(parts[-2])
                    can_id = int(parts[1], 16)  # Convert hex to int
                    data = [int(x, 16) for x in parts[3:11]]  # Convert hex data bytes
                    
                    # Create message object with raw data
                    message = {
                        "timestamp": timestamp,
                        "can_id": hex(can_id),
                        "raw_data": data,
                    }
                    
                    # Try to decode message using DBC if possible
                    try:
                        decoded_msg = db.decode_message(can_id, data)
                        message["decoded_data"] = decoded_msg
                    except:
                        # If decoding fails, store raw data only
                        print(f"Unable to decode message with ID {hex(can_id)}, storing raw data only")
                    
                    # Group by timestamp (rounded to nearest second)
                    ts_key = str(int(timestamp))
                    if ts_key not in messages_by_timestamp:
                        messages_by_timestamp[ts_key] = []
                    
                    messages_by_timestamp[ts_key].append(message)
                    
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    continue
        
        # Save grouped messages to JSON files
        files_created = 0
        for ts, messages in messages_by_timestamp.items():
            # Create filename based on timestamp
            filename = f"can_messages_{ts}.json"
            file_path = os.path.join(output_dir, filename)
            
            # Add unique IDs for Cosmos DB
            for msg in messages:
                msg['id'] = str(uuid.uuid4())
            
            # Save to JSON file
            with open(file_path, 'w') as json_file:
                json.dump(messages, json_file, indent=2)
            
            files_created += 1
        
        print(f"Successfully created {files_created} JSON files in {output_dir}")
        
        if files_created > 0:
            # Upload to Azure Blob Storage
            upload_to_azure(output_dir)
            
            # Upload to Cosmos DB
            upload_to_cosmos(output_dir)
        
        return files_created
        
    except Exception as e:
        print(f"Error processing CAN data: {str(e)}")
        return 0

def main():
    # Define paths
    raw_can_file = "raw_can_files/2-13-25.txt"
    dbc_files = [
        "dbc_files/EV3_Vehicle_Bus.dbc",
        "dbc_files/RMS.dbc"
    ]
    output_dir = "processed_can_data"
    
    # Process CAN data and upload to Azure
    if os.path.exists(raw_can_file):
        print(f"Processing {raw_can_file}...")
        files_created = process_can_data_and_upload(raw_can_file, dbc_files, output_dir)
        
        if files_created > 0:
            print("Processing and upload complete.")
        else:
            print("No files were created. Check for errors above.")
    else:
        print(f"Error: CAN data file {raw_can_file} not found.")

if __name__ == "__main__":
    main()

