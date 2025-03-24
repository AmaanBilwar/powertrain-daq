from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from psycopg2 import errors

# Load environment variables
load_dotenv()

# Get the connection string from environment variable
connection_string = os.getenv('DATABASE_URL')
if not connection_string:
    raise ValueError("DATABASE_URL not found in environment variables")

print(f"Connecting to database with: {connection_string.split('@')[0].split(':')[0]}:****@{connection_string.split('@')[1]}")

try:
    # Create a connection pool
    connection_pool = pool.SimpleConnectionPool(
        1,  # Minimum number of connections
        10,  # Maximum number of connections
        connection_string
    )

    # Check if the pool was created successfully
    if connection_pool:
        print("Connection pool created successfully")

    # Get a connection from the pool
    conn = connection_pool.getconn()

    # Create a cursor object
    cur = conn.cursor()

    # Execute SQL commands
    cur.execute('SELECT NOW();')
    time = cur.fetchone()[0]

    cur.execute('SELECT version();')
    version = cur.fetchone()[0]

    # Print the results
    print('Current time:', time)
    print('PostgreSQL version:', version)

    # Create the can_messages table if it doesn't exist
    try:
        print("Checking if can_messages table exists...")
        cur.execute("SELECT * FROM can_messages LIMIT 1")
        result = cur.fetchone()
        if result:
            print("Data from can_messages:", result)
        else:
            print("No data found in can_messages table")
    except psycopg2.errors.UndefinedRelation:
        print("Table 'can_messages' does not exist. Creating it...")
        
        # Creating the can_messages table - adjust fields as needed for your data
        create_table_query = """
        CREATE TABLE can_messages (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            can_id VARCHAR(10) NOT NULL,
            message_data JSONB NOT NULL,
            description TEXT
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'can_messages' created successfully")

    # Close the cursor and return the connection to the pool
    cur.close()
    connection_pool.putconn(conn)

except Exception as e:
    print(f"Database connection error: {e}")
finally:
    # Make sure to close the pool in a finally block
    if 'connection_pool' in locals() and connection_pool:
        connection_pool.closeall()
        print("Connection pool closed")

# Now set up Flask app to serve data
app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/messages', methods=['GET'])
def get_messages():
    try:
        # Get a connection from the pool
        conn = connection_pool.getconn()
        cur = conn.cursor()
        
        # Query messages
        cur.execute("SELECT * FROM can_messages ORDER BY timestamp DESC LIMIT 100")
        rows = cur.fetchall()
        
        # Convert to list of dictionaries
        messages = []
        for row in rows:
            messages.append({
                'id': row[0],
                'timestamp': row[1].isoformat(),
                'can_id': row[2],
                'message_data': row[3],
                'description': row[4]
            })
            
        return jsonify(messages)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        connection_pool.putconn(conn)

if __name__ == '__main__':
    app.run(debug=True)


