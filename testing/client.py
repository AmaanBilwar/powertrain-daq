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

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
CAN_INTERFACE = "can0"  # Change this to match your CAN interface
SERVER_URI = os.getenv(
    "SERVER_URI", "ws://127.0.0.1:8000/ws"
)  # Using localhost IP address
TEST_MODE = os.getenv("TEST_MODE", True)  # Set to True to simulate (fake) CAN messages

# Global variables for message lookup
rms_messages = {}
ev3_messages = {}

# Load DBC files
try:
    # Get the project root directory (two levels up from the current file)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    rms_path = os.path.join("dbc_files", "RMS.dbc")
    ev3_path = os.path.join("dbc_files", "EV3_Vehicle_Bus.dbc")

    logger.info(f"Loading DBC files from: {project_root}")
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
            "signals": decoded_message[
                "signals"
            ],  # Now contains both definitions and values
            "raw_data": decoded_message["raw_data"],
            "dlc": decoded_message["dlc"],
            "is_extended_id": message.is_extended_id,
            "is_remote_frame": message.is_remote_frame,
            "is_error_frame": message.is_error_frame,
            "is_test": TEST_MODE,
        }
    return None


async def send_can_message(websocket, message):
    """Send CAN message to the WebSocket server"""
    try:
        formatted_message = format_can_message(message)
        await websocket.send(json.dumps(formatted_message))
        logger.info(f"Sent CAN message: {formatted_message}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")


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


async def main():
    """Main function to start the client"""
    try:
        if TEST_MODE:
            logger.info("Running in TEST MODE - simulating CAN messages")
        else:
            logger.info("Running in REAL MODE - connecting to actual CAN bus")
        await connect_to_server()
    except KeyboardInterrupt:
        logger.info("Client shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
