import sqlite3
from datetime import datetime
import json

def init_database():
    """Initialize the SQLite database and create necessary tables"""
    conn = sqlite3.connect('can_messages.db')
    cursor = conn.cursor()
    
    # Create table for CAN messages
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS can_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            server_timestamp TEXT NOT NULL,
            can_id TEXT NOT NULL,
            message_name TEXT,
            signals TEXT NOT NULL,
            dbc_file TEXT
        )
    ''')
    
    conn.commit()
    return conn

def store_can_message(conn, message):
    """Store a CAN message in the database"""
    cursor = conn.cursor()
    
    # Extract message components
    timestamp = message.get('timestamp', datetime.now().isoformat())
    server_timestamp = message.get('server_timestamp', datetime.now().isoformat())
    can_id = message.get('can_id', '')
    message_name = message.get('name', '')
    signals = json.dumps(message.get('signals', {}))
    dbc_file = message.get('dbc_file', '')
    
    # Insert the message
    cursor.execute('''
        INSERT INTO can_messages 
        (timestamp, server_timestamp, can_id, message_name, signals, dbc_file)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (timestamp, server_timestamp, can_id, message_name, signals, dbc_file))
    
    conn.commit()

def get_recent_messages(conn, limit=100):
    """Retrieve recent CAN messages from the database"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM can_messages 
        ORDER BY server_timestamp DESC 
        LIMIT ?
    ''', (limit,))
    
    messages = []
    for row in cursor.fetchall():
        messages.append({
            'id': row[0],
            'timestamp': row[1],
            'server_timestamp': row[2],
            'can_id': row[3],
            'message_name': row[4],
            'signals': json.loads(row[5]),
            'dbc_file': row[6]
        })
    
    return messages 