import os
import socket
import requests
import time
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from marple import Marple
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_internet_connection(host="8.8.8.8", port=53, timeout=3):
    """
    Check if there is an internet connection by trying to connect to Google's DNS server.
    Returns True if connection is successful, False otherwise.
    """
    try:
        # Try to establish a socket connection to Google's DNS
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as e:
        logger.debug(f"Internet connection check failed: {e}")
        return False

def check_marple_connection(token=None):
    """
    Check if Marple API is accessible.
    Returns True if connection is successful, False otherwise.
    """
    if not token:
        token = os.getenv("MARPLE_ACCESS_TOKEN")
        if not token:
            logger.warning("MARPLE_ACCESS_TOKEN not found in environment variables")
            return False
    
    try:
        m = Marple(token)
        m.check_connection()
        return True
    except Exception as e:
        logger.debug(f"Marple connection check failed: {e}")
        return False

def get_last_synced_id(sync_status_path):
    """
    Get the ID of the last CAN message that was synced to Marple.
    """
    try:
        if os.path.exists(sync_status_path):
            with open(sync_status_path, 'r') as f:
                return int(f.read().strip())
        return 0
    except Exception as e:
        logger.error(f"Error reading last synced ID: {e}")
        return 0

def update_last_synced_id(sync_status_path, message_id):
    """
    Update the ID of the last CAN message that was synced to Marple.
    """
    try:
        with open(sync_status_path, 'w') as f:
            f.write(str(message_id))
    except Exception as e:
        logger.error(f"Error updating last synced ID: {e}")

def get_new_messages_from_db(conn, last_synced_id, limit=1000):
    """
    Get CAN messages that have not been synced to Marple yet.
    """
    cursor = conn.cursor()
    
    # Get messages that have not been synced yet
    cursor.execute('''
        SELECT m.id, m.timestamp, m.server_timestamp, m.can_id, 
               m.message_name, m.dbc_file,
               s.signal_name, s.value, s.unit, s.comment
        FROM can_messages m
        LEFT JOIN signals s ON m.id = s.message_id
        WHERE m.id > ?
        ORDER BY m.id, s.signal_name
        LIMIT ?
    ''', (last_synced_id, limit))
    
    rows = cursor.fetchall()
    
    # Process the rows to create a dictionary of messages
    messages = {}
    for row in rows:
        message_id = row[0]
        if message_id not in messages:
            messages[message_id] = {
                'id': message_id,
                'timestamp': row[1],
                'server_timestamp': row[2],
                'can_id': row[3],
                'message_name': row[4],
                'dbc_file': row[5],
                'signals': {}
            }
        
        if row[6]:  # If there are signals
            signal_name = row[6]
            messages[message_id]['signals'][signal_name] = {
                'value': row[7],
                'unit': row[8] or '',
                'comment': row[9] or ''
            }
    
    return list(messages.values())

def convert_messages_to_dataframe(messages):
    """
    Convert CAN messages to a pandas DataFrame in wide format suitable for Marple.
    """
    # Prepare data for DataFrame
    data = []
    
    for message in messages:
        row = {'timestamp': message['timestamp']}
        
        for signal_name, signal_data in message['signals'].items():
            # Create a column for each signal value
            row[signal_name] = signal_data['value']
        
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Ensure timestamp column exists
    if 'timestamp' not in df.columns:
        df['timestamp'] = datetime.now().isoformat()
    
    return df

def upload_to_marple(token, data, source_name, folder_path="/powertrain-daq"):
    """
    Upload data to Marple and import it.
    Returns source_id if successful, None otherwise.
    """
    import tempfile
    
    try:
        # Initialize Marple client
        m = Marple(token)
        
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file_path = temp_file.name
            data.to_csv(temp_file_path, index=False)
        
        # Upload the file
        source_id = m.upload_data_file(
            temp_file_path,
            folder_path,
            metadata={
                'source': source_name,
                'description': f'CAN signals uploaded at {datetime.now().isoformat()}',
                'device': 'raspberry-pi-telemetry'
            }
        )
        logger.info(f"Successfully uploaded {source_name} to Marple")
        
        # Import the data
        path = f"{folder_path}/{os.path.basename(temp_file_path)}"
        m.post(
            "/library/file/import",
            json={
                "path": path,
                "plugin": "csv",
                "config": {"common": [{"name": "time_column", "value": "timestamp"}]}
            }
        )
        
        # Check import status
        status = m.check_import_status(source_id)
        logger.info(f"Import status for {source_name}: {status}%")
        
        # Clean up the temporary file
        os.unlink(temp_file_path)
        
        return source_id
    except Exception as e:
        logger.error(f"Error uploading to Marple: {e}")
        return None 