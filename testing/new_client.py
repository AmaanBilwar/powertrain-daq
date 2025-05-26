import asyncio
import websockets
import json
import can
import os
import logging
from datetime import datetime
import sys
import random
import cantools
import sqlite3
import pandas as pd
from marple import Marple
from dotenv import load_dotenv
import time
import requests
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
CAN_INTERFACE = "can0"  # Change this to match your CAN interface
SERVER_URI = os.getenv("SERVER_URI", "ws://127.0.0.1:8000/ws")
TEST_MODE = os.getenv("TEST_MODE", True)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "can_messages.db")
MARPLE_SYNC_INTERVAL = 300  # 5 minutes in seconds
INTERNET_CHECK_INTERVAL = 60  # 1 minute in seconds

# Global variables for message lookup
rms_messages = {}
ev3_messages = {}

# Load DBC files
try:
    # Get the project root directory (two levels up from the current file)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    rms_path = os.path.join("dbc_files", "RMS.dbc")
    ev3_path = os.path.join("dbc_files", "EV3_Vehicle_Bus.dbc")

    logger.info(f"Loading DBC files from: {'dbc_files dir'}")
    logger.info(f"RMS DBC path: {rms_path}")
    logger.info(f"EV3 DBC path: {ev3_path}")

    dbc_rms = cantools.database.load_file(rms_path)
    dbc_ev3 = cantools.database.load_file(ev3_path)

    # Create message lookup dictionaries
    rms_messages = {msg.frame_id: msg for msg in dbc_rms.messages}
    ev3_messages = {msg.frame_id: msg for msg in dbc_ev3.messages}

    # Print available messages for reference
    logger.info("\nAvailable RMS Messages:")
    for msg_id, msg in rms_messages.items():
        logger.info(f"ID: {msg_id} (0x{msg_id:x}) - {msg.name}")
        for signal in msg.signals:
            logger.info(
                f"  └─ {signal.name}: {signal.comment or ''} [{signal.unit or 'no unit'}]"
            )

    logger.info("\nAvailable EV3 Messages:")
    for msg_id, msg in ev3_messages.items():
        logger.info(f"ID: {msg_id} (0x{msg_id:x}) - {msg.name}")
        for signal in msg.signals:
            logger.info(
                f"  └─ {signal.name}: {signal.comment or ''} [{signal.unit or 'no unit'}]"
            )

    logger.info("Successfully loaded DBC files")
except Exception as e:
    logger.error(f"Error loading DBC files: {e}")
    sys.exit(1)

def get_message_info(message_id):
    """Get human-readable message information from DBC files"""
    if message_id in rms_messages:
        msg = rms_messages[message_id]
        return {
            "name": msg.name,
            "source": "RMS(motor controller)",
            "signals": {
                sig.name: {"unit": sig.unit, "comment": sig.comment}
                for sig in msg.signals
            },
        }
    elif message_id in ev3_messages:
        msg = ev3_messages[message_id]
        return {
            "name": msg.name,
            "source": "EV3(vehicle bus)",
            "signals": {
                sig.name: {"unit": sig.unit, "comment": sig.comment}
                for sig in msg.signals
            },
        }
    return None

def decode_can_message(message):
    """Decode a CAN message using the appropriate DBC file"""
    try:
        msg_info = get_message_info(message.arbitration_id)
        if not msg_info:
            # If not found in either DBC, return raw message
            return {
                "source": "UNKNOWN",
                "message_id": hex(message.arbitration_id),
                "data": message.data.hex(),
                "raw_data": message.data.hex(),
                "dlc": message.dlc,
                "timestamp": datetime.now().isoformat(),
            }

        # Try to decode with appropriate DBC
        try:
            if msg_info["source"] == "RMS(motor controller)":
                decoded_values = dbc_rms.decode_message(
                    message.arbitration_id, message.data
                )
            else:
                decoded_values = dbc_ev3.decode_message(
                    message.arbitration_id, message.data
                )

            # Flatten signal definitions with actual values
            signals_with_values = {}
            for signal_name, signal_info in msg_info["signals"].items():
                # Create flattened keys for each signal property
                signals_with_values[f"{signal_name}_value"] = decoded_values.get(
                    signal_name, "N/A"
                )
                signals_with_values[f"{signal_name}_unit"] = (
                    signal_info["unit"] or "no unit"
                )
                signals_with_values[f"{signal_name}_comment"] = (
                    signal_info["comment"] or ""
                )

            return {
                "source": msg_info["source"],
                "message_id": hex(message.arbitration_id),
                "message_name": msg_info["name"],
                "signals": signals_with_values,  # Flattened signal info and values
                "raw_data": message.data.hex(),
                "dlc": message.dlc,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as decode_error:
            logger.error(f"Error decoding message {msg_info['name']}: {decode_error}")
            return None

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def format_can_message(message):
    """Format a CAN message into a JSON-serializable dictionary"""
    decoded_message = decode_can_message(message)
    if decoded_message:
        return {
            "timestamp": decoded_message["timestamp"],
            "source": decoded_message["source"],
            "message_id": decoded_message["message_id"],
            "message_name": decoded_message.get("message_name", "Unknown"),
            "signals": decoded_message["signals"],
            "raw_data": decoded_message["raw_data"],
            "dlc": decoded_message["dlc"],
            "is_extended_id": message.is_extended_id,
            "is_remote_frame": message.is_remote_frame,
            "is_error_frame": message.is_error_frame,
            "is_test": TEST_MODE,
        }
    return None

async def simulate_can_messages(websocket):
    """Simulate CAN messages for testing based on DBC specifications"""
    logger.info("Running in test mode - simulating CAN messages")
    try:
        while True:
            # Generate messages for different CAN IDs
            messages = []

            # RMS Messages
            # M165 Motor Position Info
            messages.append(
                can.Message(
                    arbitration_id=165,
                    data=bytes(
                        [
                            random.randint(0, 255),  # Motor Angle Electrical
                            random.randint(0, 255),
                            random.randint(0, 255),  # Motor Speed
                            random.randint(0, 255),
                            random.randint(0, 255),  # Electrical Output Frequency
                            random.randint(0, 255),
                            random.randint(0, 255),  # Delta Resolver Filtered
                            random.randint(0, 255),
                        ]
                    ),
                    dlc=8,
                    is_extended_id=False,
                )
            )

            # M166 Current Info
            messages.append(
                can.Message(
                    arbitration_id=166,
                    data=bytes(
                        [
                            random.randint(0, 255),  # Phase A Current
                            random.randint(0, 255),
                            random.randint(0, 255),  # Phase B Current
                            random.randint(0, 255),
                            random.randint(0, 255),  # Phase C Current
                            random.randint(0, 255),
                            random.randint(0, 255),  # DC Bus Current
                            random.randint(0, 255),
                        ]
                    ),
                    dlc=8,
                    is_extended_id=False,
                )
            )

            # EV3 Messages
            # APPS Info
            messages.append(
                can.Message(
                    arbitration_id=4,
                    data=bytes(
                        [
                            random.randint(0, 255),  # APPS0
                            random.randint(0, 255),
                            random.randint(0, 255),  # APPS1
                            random.randint(0, 255),
                            random.randint(0, 100),  # APPS_Pct
                            random.randint(0, 255),  # Torque_Cmd
                            random.randint(0, 255),
                            0,
                        ]
                    ),
                    dlc=8,
                    is_extended_id=False,
                )
            )

            # Sensors Info
            messages.append(
                can.Message(
                    arbitration_id=5,
                    data=bytes(
                        [
                            random.randint(0, 255),  # BPS_Raw
                            random.randint(0, 255),
                            random.randint(20, 80),  # Water1_Temp_C
                            random.randint(20, 80),  # Water2_Temp_C
                            random.randint(20, 80),  # Water3_Temp_C
                            0,  # Reserved
                            random.randint(0, 1) << 7,  # R2D_Button and other buttons
                            0,
                        ]
                    ),
                    dlc=8,
                    is_extended_id=False,
                )
            )

            # Send all messages
            for msg in messages:
                await send_can_message(websocket, msg)
                await asyncio.sleep(0.1)  # Small delay between messages

            await asyncio.sleep(1)  # Wait 1 second before next batch

    except Exception as e:
        logger.error(f"Error in test mode: {e}")

async def real_can_listener(websocket):
    """Listen for real CAN messages and send them to the server"""
    try:
        # Initialize CAN bus
        bus = can.interface.Bus(channel=CAN_INTERFACE, bustype="socketcan")
        logger.info(f"Connected to CAN bus {CAN_INTERFACE}")

        while True:
            message = bus.recv(timeout=1.0)
            if message is not None:
                await send_can_message(websocket, message)

    except Exception as e:
        logger.error(f"Error in CAN listener: {e}")
    finally:
        if "bus" in locals():
            bus.shutdown()

async def connect_to_server():
    """Establish connection to the WebSocket server"""
    while True:
        try:
            logger.info(f"Attempting to connect to {SERVER_URI}")
            async with websockets.connect(SERVER_URI) as websocket:
                logger.info("Successfully connected to WebSocket server")
                if TEST_MODE:
                    await simulate_can_messages(websocket)
                else:
                    await real_can_listener(websocket)
        except websockets.exceptions.InvalidStatusCode as e:
            logger.error(f"Connection failed: {e}")
            logger.info("Make sure the server is running and accessible")
        except ConnectionRefusedError:
            logger.error("Connection refused. Is the server running?")
        except Exception as e:
            logger.error(f"Connection error: {e}")
        logger.info("Retrying in 5 seconds...")
        await asyncio.sleep(5)

def init_database():
    """Initialize SQLite database with required tables"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create can_messages table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS can_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            source TEXT,
            message_id TEXT,
            message_name TEXT,
            raw_data TEXT,
            dlc INTEGER,
            is_extended_id BOOLEAN,
            is_remote_frame BOOLEAN,
            is_error_frame BOOLEAN,
            is_test BOOLEAN,
            uploaded_to_marple BOOLEAN DEFAULT 0
        )
    ''')
    
    # Create signals table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            signal_name TEXT,
            value REAL,
            unit TEXT,
            FOREIGN KEY (message_id) REFERENCES can_messages(id)
        )
    ''')
    
    conn.commit()
    conn.close()

def save_to_database(formatted_message):
    """Save CAN message and its signals to the local database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Insert into can_messages table
        cursor.execute('''
            INSERT INTO can_messages (
                timestamp, source, message_id, message_name, raw_data,
                dlc, is_extended_id, is_remote_frame, is_error_frame, is_test
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            formatted_message['timestamp'],
            formatted_message['source'],
            formatted_message['message_id'],
            formatted_message.get('message_name', 'Unknown'),
            formatted_message['raw_data'],
            formatted_message['dlc'],
            formatted_message['is_extended_id'],
            formatted_message['is_remote_frame'],
            formatted_message['is_error_frame'],
            formatted_message['is_test']
        ))
        
        message_id = cursor.lastrowid
        
        # Insert signals
        if 'signals' in formatted_message:
            for signal_name, value in formatted_message['signals'].items():
                if signal_name.endswith('_value'):
                    base_name = signal_name[:-6]  # Remove '_value' suffix
                    unit = formatted_message['signals'].get(f'{base_name}_unit', '')
                    
                    cursor.execute('''
                        INSERT INTO signals (message_id, signal_name, value, unit)
                        VALUES (?, ?, ?, ?)
                    ''', (message_id, base_name, value, unit))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        return False

def check_internet_connection():
    """Check if internet connection is available"""
    try:
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        return False

def get_marple_token():
    """Get Marple access token from environment variables"""
    token = os.getenv("MARPLE_ACCESS_TOKEN")
    if not token:
        raise ValueError("MARPLE_ACCESS_TOKEN not found in environment variables")
    return token

def get_merged_signals():
    """Read and join signals with can_messages to include timestamp for Marple upload"""
    conn = sqlite3.connect(DB_PATH)
    
    # Read both tables
    can_messages_df = pd.read_sql_query(
        "SELECT id, timestamp FROM can_messages WHERE uploaded_to_marple = 0", 
        conn
    )
    signals_df = pd.read_sql_query("SELECT * FROM signals", conn)
    
    # Merge signals with can_messages to get timestamp for each signal
    merged = signals_df.merge(
        can_messages_df,
        left_on="message_id",
        right_on="id",
        suffixes=("_signal", "_can")
    )
    
    # Select and rename columns for Marple
    marple_df = merged.rename(columns={
        "timestamp": "timestamp",
        "signal_name": "signal",
        "value": "value",
        "unit": "unit"
    })
    marple_df = marple_df[["timestamp", "signal", "value", "unit", "message_id"]]
    
    conn.close()
    return marple_df

def upload_to_marple(m, data, source_name, folder_path="/powertrain-daq"):
    """Upload data to Marple using CSV file"""
    import tempfile
    
    try:
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file_path = temp_file.name
            data.to_csv(temp_file_path, index=False)
            
        # Upload the file
        source_id = m.upload_data_file(
            temp_file_path,
            folder_path,
            metadata={
                "source": source_name,
                "description": "Signals with timestamp for Marple upload"
            }
        )
        
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
        
        # Mark messages as uploaded in database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE can_messages SET uploaded_to_marple = 1 WHERE id IN (?)",
            (tuple(data['message_id'].unique()),)
        )
        conn.commit()
        conn.close()
        
        return source_id
    except Exception as e:
        logger.error(f"Error uploading to Marple: {e}")
        return None

async def handle_can_messages():
    """Handle CAN messages independently of internet connection"""
    try:
        if TEST_MODE:
            logger.info("Running in TEST MODE - simulating CAN messages")
            while True:
                # Generate test messages
                messages = []
                # RMS Messages
                # M165 Motor Position Info
                messages.append(
                    can.Message(
                        arbitration_id=165,
                        data=bytes(
                            [
                                random.randint(0, 255),  # Motor Angle Electrical
                                random.randint(0, 255),
                                random.randint(0, 255),  # Motor Speed
                                random.randint(0, 255),
                                random.randint(0, 255),  # Electrical Output Frequency
                                random.randint(0, 255),
                                random.randint(0, 255),  # Delta Resolver Filtered
                                random.randint(0, 255),
                            ]
                        ),
                        dlc=8,
                        is_extended_id=False,
                    )
                )

                # M166 Current Info
                messages.append(
                    can.Message(
                        arbitration_id=166,
                        data=bytes(
                            [
                                random.randint(0, 255),  # Phase A Current
                                random.randint(0, 255),
                                random.randint(0, 255),  # Phase B Current
                                random.randint(0, 255),
                                random.randint(0, 255),  # Phase C Current
                                random.randint(0, 255),
                                random.randint(0, 255),  # DC Bus Current
                                random.randint(0, 255),
                            ]
                        ),
                        dlc=8,
                        is_extended_id=False,
                    )
                )

                # EV3 Messages
                # APPS Info
                messages.append(
                    can.Message(
                        arbitration_id=4,
                        data=bytes(
                            [
                                random.randint(0, 255),  # APPS0
                                random.randint(0, 255),
                                random.randint(0, 255),  # APPS1
                                random.randint(0, 255),
                                random.randint(0, 100),  # APPS_Pct
                                random.randint(0, 255),  # Torque_Cmd
                                random.randint(0, 255),
                                0,
                            ]
                        ),
                        dlc=8,
                        is_extended_id=False,
                    )
                )

                # Sensors Info
                messages.append(
                    can.Message(
                        arbitration_id=5,
                        data=bytes(
                            [
                                random.randint(0, 255),  # BPS_Raw
                                random.randint(0, 255),
                                random.randint(20, 80),  # Water1_Temp_C
                                random.randint(20, 80),  # Water2_Temp_C
                                random.randint(20, 80),  # Water3_Temp_C
                                0,  # Reserved
                                random.randint(0, 1) << 7,  # R2D_Button and other buttons
                                0,
                            ]
                        ),
                        dlc=8,
                        is_extended_id=False,
                    )
                )

                # Process all messages
                for msg in messages:
                    formatted_message = format_can_message(msg)
                    if formatted_message:
                        save_to_database(formatted_message)
                        logger.debug(f"Saved message to database: {formatted_message['message_name']}")
                
                await asyncio.sleep(1)  # Wait 1 second before next batch
        else:
            logger.info("Running in REAL MODE - connecting to actual CAN bus")
            bus = can.interface.Bus(channel=CAN_INTERFACE, bustype="socketcan")
            logger.info(f"Connected to CAN bus {CAN_INTERFACE}")
            
            while True:
                message = bus.recv(timeout=1.0)
                if message is not None:
                    formatted_message = format_can_message(message)
                    if formatted_message:
                        save_to_database(formatted_message)
                        logger.debug(f"Saved message to database: {formatted_message['message_name']}")
    except Exception as e:
        logger.error(f"Error in CAN message handling: {e}")
    finally:
        if not TEST_MODE and "bus" in locals():
            bus.shutdown()

async def sync_with_server():
    """Sync data with server when internet is available"""
    while True:
        try:
            if check_internet_connection():
                logger.info("Internet connection available, attempting to sync with server...")
                try:
                    async with websockets.connect(SERVER_URI, ping_interval=30, ping_timeout=10) as websocket:
                        # Get unsynced messages from database
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM can_messages WHERE uploaded_to_marple = 0")
                        unsynced_messages = cursor.fetchall()
                        conn.close()

                        if unsynced_messages:
                            logger.info(f"Found {len(unsynced_messages)} messages to sync with server")
                            for message in unsynced_messages:
                                try:
                                    await websocket.send(json.dumps(message))
                                    logger.info(f"Synced message with server: {message}")
                                    await asyncio.sleep(0.1)  # Small delay between messages
                                except websockets.exceptions.ConnectionClosed:
                                    logger.warning("Connection closed while syncing messages")
                                    break
                                except Exception as ws_error:
                                    logger.warning(f"Error sending message to server: {ws_error}")
                                    break
                        else:
                            logger.info("No new messages to sync with server")
                except websockets.exceptions.InvalidStatusCode as e:
                    logger.error(f"Server connection failed: {e}")
                except ConnectionRefusedError:
                    logger.error("Server connection refused. Is the server running?")
                except Exception as e:
                    logger.error(f"Server connection error: {e}")
            else:
                logger.info("No internet connection available, skipping server sync")
            
            await asyncio.sleep(INTERNET_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Error in server sync: {e}")
            await asyncio.sleep(INTERNET_CHECK_INTERVAL)

async def sync_to_marple():
    """Periodically check internet connection and sync data to Marple"""
    while True:
        try:
            if check_internet_connection():
                logger.info("Internet connection available, attempting to sync with Marple...")
                token = get_marple_token()
                m = Marple(token)
                
                # Get data that hasn't been uploaded yet
                signals_data = get_merged_signals()
                if not signals_data.empty:
                    logger.info(f"Found {len(signals_data)} new signals to upload")
                    upload_to_marple(
                        m,
                        signals_data,
                        "can_signals",
                        "/powertrain-daq/can_signals"
                    )
                else:
                    logger.info("No new data to upload to Marple")
            else:
                logger.info("No internet connection available, skipping Marple sync")
            
            await asyncio.sleep(MARPLE_SYNC_INTERVAL)
        except Exception as e:
            logger.error(f"Error in Marple sync: {e}")
            await asyncio.sleep(MARPLE_SYNC_INTERVAL)

async def send_can_message(websocket, message):
    """Send CAN message to the WebSocket server and save to local database"""
    try:
        formatted_message = format_can_message(message)
        if formatted_message:
            # Save to local database
            save_to_database(formatted_message)
            
            # Try to send to WebSocket if available
            try:
                await websocket.send(json.dumps(formatted_message))
                logger.info(f"Sent CAN message: {formatted_message}")
            except Exception as ws_error:
                logger.warning(f"Could not send to WebSocket: {ws_error}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def main():
    """Main function to start the client"""
    try:
        # Initialize database
        init_database()
        
        # Create tasks for parallel execution
        can_task = asyncio.create_task(handle_can_messages())
        server_sync_task = asyncio.create_task(sync_with_server())
        marple_sync_task = asyncio.create_task(sync_to_marple())
        
        # Wait for all tasks to complete (they should run indefinitely)
        await asyncio.gather(can_task, server_sync_task, marple_sync_task)
        
    except KeyboardInterrupt:
        logger.info("Client shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    asyncio.run(main()) 