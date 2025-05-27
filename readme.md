# CAN Data Acquisition System

A real-time CAN bus data acquisition and monitoring system that captures, decodes, and stores CAN messages from both motor controller (RMS) and vehicle bus (EV3) networks, with offline support and Marple dashboard integration.

## System Architecture

The system consists of these main components:

1. **Client (`client.py`)**: Connects to the CAN bus, stores messages locally, and forwards to server when available
2. **Server (`server.py`)**: WebSocket server that receives and processes CAN messages (optional)
3. **Database (`database.py`)**: SQLite database for storing CAN messages and signals
4. **Sync Manager (`sync_manager.py`)**: Background process that syncs data to Marple when internet is available
5. **Network Utilities (`network_utils.py`)**: Utilities for checking connectivity and sending data to Marple

## Features

- Real-time CAN message capture and decoding
- Support for both real CAN bus and simulation modes
- DBC file support for message decoding (RMS and EV3 networks)
- WebSocket-based communication (optional)
- SQLite database storage
- Detailed signal information including units and comments
- Test mode for simulation and development
- **Offline operation with local storage**
- **Automatic synchronization with Marple dashboard when internet is available**
- **Raspberry Pi optimized for telemetry applications**

## Components

### Client (`client.py`)

The client component is responsible for:
- Connecting to the CAN bus interface
- Decoding CAN messages using DBC files
- Storing messages in local database
- Optionally forwarding messages to WebSocket server
- Supporting both real and simulated CAN messages
- Automatic reconnection to server (if WebSocket mode enabled)
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

### Sync Manager (`sync_manager.py`)

The sync manager handles:
- Periodic internet connectivity checks
- Retrieving unsynchronized data from local database
- Converting data to Marple-compatible format
- Uploading data to Marple dashboard when internet is available
- Tracking synchronization status

### Network Utilities (`network_utils.py`)

Provides utilities for:
- Checking internet connectivity
- Checking Marple API availability
- Converting CAN messages to DataFrame format
- Uploading data to Marple

## Configuration

### Environment Variables

The system can be configured via environment variables or a `.env` file:

- `SERVER_URI`: WebSocket server address (default: "ws://127.0.0.1:8000/ws")
- `TEST_MODE`: Enable/disable simulation mode (default: "true")
- `WEBSOCKET_MODE`: Enable/disable WebSocket server connection (default: "false")
- `CAN_INTERFACE`: CAN bus interface name (default: 'can0')
- `SYNC_INTERVAL`: How often to check for internet and sync data (in seconds, default: 300)
- `MARPLE_ACCESS_TOKEN`: API token for Marple dashboard (required for syncing)

### DBC Files

The system requires two DBC files:
- `RMS.dbc`: Motor controller message definitions
- `EV3_Vehicle_Bus.dbc`: Vehicle bus message definitions

These should be placed in the `testing/dbc_files` directory.

## Usage

### Starting the Telemetry Client

Use the wrapper script for easy configuration:

```bash
python start_telemetry.py [options]
```

Options:
- `--test`: Run in test mode (simulate CAN messages)
- `--websocket`: Enable connection to WebSocket server
- `--server-uri URI`: Specify WebSocket server URI
- `--interface NAME`: Specify CAN interface name
- `--sync-interval SECONDS`: Specify sync interval in seconds

### For Development (in the `testing` directory)

1. Start the server (optional, only if using WebSocket mode): 
   ```bash
   python server.py
   ```

2. Start the client directly:
   ```bash
   python client.py
   ```

### Marple Dashboard Integration

1. Ensure you have a Marple account and API token
2. Set the `MARPLE_ACCESS_TOKEN` environment variable or add it to your `.env` file
3. The system will automatically sync data to Marple when internet is available
4. Access your data in the Marple dashboard for visualization and analysis

## Offline Operation

The system is designed to work offline on a Raspberry Pi:

1. The client captures and stores CAN messages in a local SQLite database
2. The sync manager periodically checks for internet connectivity
3. When internet becomes available, data is automatically synced to Marple
4. Synchronization status is tracked to ensure no data is lost or duplicated

## Dependencies

- Python 3.x
- `websockets`
- `can`
- `cantools`
- `fastapi`
- `uvicorn`
- `sqlite3`
- `pandas`
- `marpledata`
- `python-dotenv`

## Logging

All components include comprehensive logging:
- Timestamp-based logging
- Different log levels (INFO, ERROR, DEBUG)
- Detailed message information
- Connection status
- Synchronization status
- Error tracking