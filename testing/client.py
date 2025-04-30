import asyncio
import websockets
import json
import can
import logging
from datetime import datetime
import sys
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CAN_INTERFACE = 'can0'  # Change this to match your CAN interface
SERVER_URI = "ws://127.0.0.1:8000/ws"  # Using localhost IP address
TEST_MODE = True  # Set to True to simulate CAN messages

async def send_raw_data(websocket, message):
    """Send raw CAN data to the WebSocket server"""
    try:
        # Create a basic message with timestamp
        data = {
            'timestamp': datetime.now().isoformat(),
            'raw_data': str(message),  # Convert the entire message to string
            'is_test': TEST_MODE
        }
        
        # Add all available attributes from the CAN message
        for attr in dir(message):
            if not attr.startswith('_') and not callable(getattr(message, attr)):
                try:
                    value = getattr(message, attr)
                    # Convert bytes to hex string if needed
                    if isinstance(value, bytes):
                        value = value.hex()
                    data[attr] = value
                except Exception as e:
                    logger.debug(f"Could not get attribute {attr}: {e}")
        
        await websocket.send(json.dumps(data))
        logger.info(f"Sent raw data: {data}")
    except Exception as e:
        logger.error(f"Error sending data: {e}")

async def simulate_can_messages(websocket):
    """Simulate CAN messages for testing"""
    logger.info("Running in test mode - simulating CAN messages")
    try:
        while True:
            # Generate random CAN message
            message = can.Message(
                arbitration_id=random.randint(0, 0x7FF),
                data=bytes([random.randint(0, 255) for _ in range(8)]),
                dlc=8,
                is_extended_id=False,
                is_remote_frame=False,
                is_error_frame=False
            )
            await send_raw_data(websocket, message)
            await asyncio.sleep(1)  # Send a message every second
    except Exception as e:
        logger.error(f"Error in test mode: {e}")

async def real_can_listener(websocket):
    """Listen for real CAN messages and send them to the server"""
    try:
        # Initialize CAN bus
        bus = can.interface.Bus(channel=CAN_INTERFACE, bustype='socketcan')
        logger.info(f"Connected to CAN bus {CAN_INTERFACE}")
        
        while True:
            message = bus.recv(timeout=1.0)
            if message is not None:
                await send_raw_data(websocket, message)
                
    except Exception as e:
        logger.error(f"Error in CAN listener: {e}")
    finally:
        if 'bus' in locals():
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