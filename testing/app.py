from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import sqlite3
import json
from typing import List, Dict, Any

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_path(db_name: str) -> str:
    """Get the full path to a database file"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    return os.path.join(data_dir, f"{db_name}.db")

def get_db_connection(db_name: str) -> sqlite3.Connection:
    """Get a connection to the specified database"""
    db_path = get_db_path(db_name)
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database not found")
    return sqlite3.connect(db_path)

@app.get("/")
async def root():
    return {"message": "CAN Message Viewer API"}

@app.get("/api/databases")
async def get_databases() -> Dict[str, List[str]]:
    """Get list of available databases"""
    try:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        if not os.path.exists(data_dir):
            return {"databases": []}
        
        db_files = [f.replace('.db', '') for f in os.listdir(data_dir) if f.endswith('.db')]
        return {"databases": db_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/columns")
async def get_columns(db: str) -> Dict[str, List[str]]:
    """Get columns from a specific database"""
    try:
        conn = get_db_connection(db)
        cursor = conn.cursor()
        
        # Get columns from can_messages table
        cursor.execute("PRAGMA table_info(can_messages)")
        message_columns = [row[1] for row in cursor.fetchall()]
        
        # Get columns from signals table
        cursor.execute("PRAGMA table_info(signals)")
        signal_columns = [row[1] for row in cursor.fetchall()]
        
        # Combine columns, prefixing signal columns with 'signals.'
        columns = message_columns + [f"signals.{col}" for col in signal_columns if col not in ['id', 'message_id']]
        
        conn.close()
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/data")
async def get_data(db: str, x: str, y: str) -> List[Dict[str, Any]]:
    """Get data for visualization"""
    try:
        conn = get_db_connection(db)
        cursor = conn.cursor()
        
        # Determine if we're querying message or signal data
        x_is_signal = x.startswith('signals.')
        y_is_signal = y.startswith('signals.')
        
        # Clean column names
        x_col = x.replace('signals.', '') if x_is_signal else x
        y_col = y.replace('signals.', '') if y_is_signal else y
        
        # Build the query based on whether we're using signal data
        if x_is_signal or y_is_signal:
            # Join can_messages and signals tables
            query = f"""
                SELECT 
                    m.{x_col if not x_is_signal else 'timestamp'} as x,
                    s.{y_col if y_is_signal else x_col} as y
                FROM can_messages m
                LEFT JOIN signals s ON m.id = s.message_id
                WHERE s.signal_name = '{y_col if y_is_signal else x_col}'
                ORDER BY m.timestamp
            """
        else:
            # Just query can_messages table
            query = f"""
                SELECT {x} as x, {y} as y
                FROM can_messages
                ORDER BY {x}
            """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        data = [{"x": row[0], "y": row[1]} for row in rows]
        
        conn.close()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
