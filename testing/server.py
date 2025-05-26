from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
from datetime import datetime
import logging
import uvicorn
from database import init_database, store_can_message
import os

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
try:
    db_conn = init_database()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")
    raise


@app.get("/")
async def root():
    return {"message": "WebSocket server is running"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle WebSocket connections and messages"""
    await websocket.accept()
    logger.info(f"New client connected. Total connections: {len(active_connections) + 1}")
    active_connections.add(websocket)
    
    try:
        while True:
            try:
                # Receive message
                data = await websocket.receive_text()
                
                try:
                    # Parse message
                    message = json.loads(data)
                    
                    # Save to database
                    store_can_message(db_conn, message)
                    
                    # Broadcast to other clients
                    await broadcast_message(message, websocket)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
            except WebSocketDisconnect:
                logger.info("Client disconnected normally")
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                break
                
    finally:
        active_connections.remove(websocket)
        logger.info(f"Client disconnected. Remaining connections: {len(active_connections)}")

async def broadcast_message(message: dict, sender: WebSocket):
    """Broadcast message to all connected clients except sender"""
    if not active_connections:
        return
        
    disconnected = set()
    for connection in active_connections:
        if connection != sender:
            try:
                await connection.send_json(message)
            except WebSocketDisconnect:
                disconnected.add(connection)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")
                disconnected.add(connection)
    
    # Clean up disconnected clients
    for connection in disconnected:
        active_connections.remove(connection)
        logger.info(f"Removed disconnected client. Remaining connections: {len(active_connections)}")


if __name__ == "__main__":
    logger.info("Starting WebSocket server...")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
