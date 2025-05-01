import sqlite3
from datetime import datetime
import json
import os

def init_database():
    """Initialize the SQLite database and create necessary tables"""
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Connect to database in the data directory
    db_path = os.path.join(data_dir, 'can_messages.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table for CAN messages
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS can_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            server_timestamp TEXT NOT NULL,
            can_id TEXT NOT NULL,
            message_name TEXT,
            dbc_file TEXT
        )
    ''')
    
    # Create table for signals
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            signal_name TEXT NOT NULL,
            value TEXT NOT NULL,
            unit TEXT,
            comment TEXT,
            FOREIGN KEY (message_id) REFERENCES can_messages(id)
        )
    ''')
    
    conn.commit()
    return conn

def store_can_message(conn, message):
    """Store a CAN message and its signals in the database"""
    cursor = conn.cursor()
    
    try:
        # Begin transaction
        cursor.execute('BEGIN TRANSACTION')
        
        # Extract message components
        timestamp = message.get('timestamp', datetime.now().isoformat())
        server_timestamp = message.get('server_timestamp', datetime.now().isoformat())
        can_id = message.get('can_id', '')
        message_name = message.get('name', '')
        dbc_file = message.get('dbc_file', '')
        
        # Insert the main message
        cursor.execute('''
            INSERT INTO can_messages 
            (timestamp, server_timestamp, can_id, message_name, dbc_file)
            VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, server_timestamp, can_id, message_name, dbc_file))
        
        # Get the ID of the inserted message
        message_id = cursor.lastrowid
        
        # Insert each signal
        signals = message.get('signals', {})
        for signal_name, signal_data in signals.items():
            if isinstance(signal_data, dict):
                # Handle nested signal structure
                value = str(signal_data.get('value', 'N/A'))
                unit = signal_data.get('unit', '')
                comment = signal_data.get('comment', '')
            else:
                # Handle flat signal structure
                value = str(signal_data)
                unit = ''
                comment = ''
            
            cursor.execute('''
                INSERT INTO signals 
                (message_id, signal_name, value, unit, comment)
                VALUES (?, ?, ?, ?, ?)
            ''', (message_id, signal_name, value, unit, comment))
        
        # Commit transaction
        conn.commit()
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        raise e

def get_recent_messages(conn, limit=100):
    """Retrieve recent CAN messages with their signals from the database"""
    cursor = conn.cursor()
    
    # Get recent messages
    cursor.execute('''
        SELECT m.id, m.timestamp, m.server_timestamp, m.can_id, 
               m.message_name, m.dbc_file,
               s.signal_name, s.value, s.unit, s.comment
        FROM can_messages m
        LEFT JOIN signals s ON m.id = s.message_id
        WHERE m.id IN (
            SELECT id FROM can_messages 
            ORDER BY server_timestamp DESC 
            LIMIT ?
        )
        ORDER BY m.server_timestamp DESC, s.signal_name
    ''', (limit,))
    
    messages = {}
    for row in cursor.fetchall():
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