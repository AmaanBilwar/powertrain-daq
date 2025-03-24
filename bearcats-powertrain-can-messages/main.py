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
    Upload JSON files from the specified directory to Azure Cosmos DB.
    
    Args:
        data_directory: Path to the directory containing JSON files to upload (default: current directory)
        database_name: Name of the Cosmos DB database
        container_name: Name of the Cosmos DB container
    """
    # Import required libraries for Cosmos DB
    from azure.cosmos import CosmosClient, exceptions, PartitionKey
    
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
            offer_throughput=400
        )
    
    # Upload JSON files in the directory
    items_uploaded = 0
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
            
            # Ensure each document has an id
            if isinstance(data, list):
                # If data is a list of items
                for item in data:
                    if 'id' not in item:
                        item['id'] = str(uuid.uuid4())
                    container.upsert_item(item)
                    items_uploaded += 1
            else:
                # If data is a single document
                if 'id' not in data:
                    data['id'] = str(uuid.uuid4())
                container.upsert_item(data)
                items_uploaded += 1
                
        except json.JSONDecodeError:
            print(f"Error: {filename} is not a valid JSON file. Skipping.")
        except Exception as e:
            print(f"Error uploading {filename}: {str(e)}")
    
    print(f"Upload complete. {items_uploaded} items uploaded to Cosmos DB container '{container_name}'")


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


def main():
    # Create directory for CAN messages if it doesn't exist
    can_messages_dir = "can-messages"
    if not os.path.exists(can_messages_dir):
        os.makedirs(can_messages_dir)
        print(f"Created directory: {can_messages_dir}")
    
    # Path to the decoded CAN messages CSV file
    csv_file_path = "decoded_can_messages.csv"
    
    # Convert CSV to JSON files
    if os.path.exists(csv_file_path):
        print(f"Converting {csv_file_path} to JSON files...")
        files_created = create_json_from_csv(csv_file_path, can_messages_dir)
        
        if files_created > 0:
            # Upload files to both Azure Blob Storage and Cosmos DB
            upload_to_azure(can_messages_dir)
            upload_to_cosmos(can_messages_dir)
        else:
            print("No files were created from CSV. Skipping upload.")
    else:
        print(f"Error: CSV file {csv_file_path} not found.")


if __name__ == "__main__":
    main()