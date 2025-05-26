# CAN Data Acquisition System

A real-time CAN bus data acquisition and monitoring system that captures, decodes, and stores CAN messages from both motor controller (RMS) and vehicle bus (EV3) networks.

## System Architecture

The system consists of three main components:

1. **Client (`client.py`)**: Connects to the CAN bus and forwards messages to the server
2. **Server (`server.py`)**: WebSocket server that receives and processes CAN messages
3. **Database (`database.py`)**: SQLite database for storing CAN messages and signals

## Features

- Real-time CAN message capture and decoding
- Support for both real CAN bus and simulation modes
- DBC file support for message decoding (RMS and EV3 networks)
- WebSocket-based communication
- SQLite database storage
- Detailed signal information including units and comments
- Test mode for simulation and development

## Components

### Client (`client.py`)

The client component is responsible for:
- Connecting to the CAN bus interface
- Decoding CAN messages using DBC files
- Formatting messages for transmission
- Supporting both real and simulated CAN messages
- Automatic reconnection to server
- Detailed logging of operations

### Server (`server.py`)

The server component provides:
- WebSocket endpoint for client connections
- Real-time message processing
- CORS support for web applications
- Message storage in database
- Connection management
- Error handling and logging

### Database (`database.py`)

The database component handles:
- SQLite database initialization
- Two-table structure:
  - `can_messages`: Stores message metadata
  - `signals`: Stores individual signal data
- Transaction-based message storage
- Query support for recent messages

## Configuration

### Environment Variables

- `SERVER_URI`: WebSocket server address (default: "ws://127.0.0.1:8000/ws")
- `TEST_MODE`: Enable/disable simulation mode (default: True)
- `CAN_INTERFACE`: CAN bus interface name (default: 'can0')

### DBC Files

The system requires two DBC files:
- `RMS.dbc`: Motor controller message definitions
- `EV3_Vehicle_Bus.dbc`: Vehicle bus message definitions

These should be placed in the `dbc_files` directory.

## Usage

### make sure you're in the `testing` dir before proceeding

1. Start the server using `python server.py`OR `uv run server.py`



2. Start the client using `python client.py` OR `uv run client.py`:

The system will automatically:
- Connect to the CAN bus (or run in simulation mode)
- Decode messages using DBC files
- Store messages in the database
- Provide real-time monitoring capabilities

## Dependencies

- Python 3.x
- `websockets`
- `can`
- `cantools`
- `fastapi`
- `uvicorn`
- `sqlite3`

## Logging

Both client and server components include comprehensive logging:
- Timestamp-based logging
- Different log levels (INFO, ERROR)
- Detailed message information
- Connection status
- Error tracking