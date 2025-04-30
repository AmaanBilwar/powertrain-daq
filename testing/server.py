from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
from datetime import datetime
import logging
import uvicorn
from database import init_database, store_can_message

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active connections
active_connections = set()

# Initialize database connection
db_conn = init_database()

@app.get("/")
async def root():
    return {"message": "WebSocket server is running"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    try:
        await websocket.accept()
        active_connections.add(websocket)
        logger.info(f"New client connected. Total connections: {len(active_connections)}")
        
        while True:
            try:
                # Receive CAN message from client
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Add timestamp to the message
                message['server_timestamp'] = datetime.now().isoformat()
                
                # Store message in database
                store_can_message(db_conn, message)
                
                # Log the message
                logger.info(f"Received and stored CAN message: {message}")
                
                # Echo the message back to the client (optional)
                await websocket.send_text(json.dumps(message))
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received: {e}")
                await websocket.send_text(json.dumps({"error": "Invalid JSON format"}))
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await websocket.send_text(json.dumps({"error": str(e)}))
                
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        active_connections.remove(websocket)
        logger.info(f"Client disconnected. Remaining connections: {len(active_connections)}")

if __name__ == "__main__":
    logger.info("Starting WebSocket server...")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True) 