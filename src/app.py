from flask import Flask, request, jsonify
from flask_cors import CORS
import os 
from dotenv import load_dotenv
import uuid
import json
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import can
import threading
import time

load_dotenv()

# load envs
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')
CAN_LOG_FILE = os.getenv('CAN_LOG_FILE')

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

app = Flask(__name__)
CORS(app)

# CAN Bus monitoring thread
def can_monitor():
    try:
        bus = can.interface.Bus(channel='can0', bustype='socketcan')
        
        # Create or open log file
        with open(CAN_LOG_FILE, 'a') as log_file:
            while True:
                message = bus.recv()
                
                # Format the CAN message
                timestamp = datetime.now().isoformat()
                can_id = message.arbitration_id
                can_data = message.data.hex()
                
                # Create log entry
                log_entry = f"{timestamp},{can_id},{can_data}\n"
                
                # Write to log file
                log_file.write(log_entry)
                log_file.flush()
                
                # Write to InfluxDB
                point = Point("can_signal") \
                    .tag("can_id", str(can_id)) \
                    .field("raw_data", can_data) \
                    .time(timestamp)
                
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                
    except Exception as e:
        print(f"CAN monitoring error: {e}")

# Route to receive CAN data from external sources (if needed)
@app.route('/api/can-data', methods=['POST'])
def receive_can_data():
    try:
        data = request.json
        required_fields = ['timestamp', 'can_id', 'data']
        
        # Validate request
        if not all(field in data for field in required_fields):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Write to log file
        with open(CAN_LOG_FILE, 'a') as log_file:
            log_entry = f"{data['timestamp']},{data['can_id']},{data['data']}\n"
            log_file.write(log_entry)
        
        # Write to InfluxDB
        point = Point("can_signal") \
            .tag("can_id", str(data['can_id'])) \
            .field("raw_data", data['data']) \
            .time(data['timestamp'])
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        
        return jsonify({"success": True}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to get latest CAN data
@app.route('/api/can-data', methods=['GET'])
def get_can_data():
    try:
        # Query the most recent data points from InfluxDB
        query = f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "can_signal") |> last()'
        tables = influx_client.query_api().query(query, org=INFLUXDB_ORG)
        
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "can_id": record.values.get("can_id"),
                    "data": record.values.get("raw_data"),
                    "timestamp": record.get_time().isoformat()
                })
        
        return jsonify(results), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Start CAN monitoring in a separate thread
    can_thread = threading.Thread(target=can_monitor, daemon=True)
    can_thread.start()
    
    # Start Flask app
    app.run(host='0.0.0.0', debug=True, port=5000)