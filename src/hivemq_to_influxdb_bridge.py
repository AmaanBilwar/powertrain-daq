"""
HiveMQ to InfluxDB Bridge - Subscribes to HiveMQ MQTT messages and stores them in InfluxDB
"""

import paho.mqtt.client as mqtt
import json
import time
import os
from dotenv import load_dotenv
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "can_telemetry")

# MQTT Configuration - use same values from your existing setup
MQTT_BROKER = os.getenv("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))  # Using TLS port as in your code
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "fsae/telemetry")
CLIENT_ID = f"fsae_influxdb_bridge_{int(time.time())}"

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback when connected to MQTT broker"""
    if rc == 0:
        logger.info(f"Connected to MQTT Broker ({MQTT_BROKER})")
        # Subscribe to the topic
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        logger.error(f"Failed to connect to MQTT Broker. Return code {rc}")

def on_message(client, userdata, msg):
    """Callback when a message is received"""
    try:
        # Decode the JSON payload
        payload = json.loads(msg.payload.decode())
        logger.info(f"Received message: ID=0x{payload.get('arbitration_id', 0):X}")
        
        # Create a point with the appropriate measurement name
        point = Point("can_message")
        
        # Add tag for arbitration_id - good for filtering in queries
        point.tag("arbitration_id", hex(payload.get('arbitration_id', 0)))
        point.tag("topic", msg.topic)
        
        # Add the fields
        point.field("dlc", payload.get("dlc", 0))
        
        # Add each byte in the data array as a separate field
        data_bytes = payload.get("data", [])
        for i, value in enumerate(data_bytes):
            point.field(f"byte_{i}", value)
        
        # You can also store the raw data as a string if needed
        point.field("raw_data", str(data_bytes))
        
        # Use the timestamp from the message if available
        timestamp = payload.get("timestamp", time.time())
        point.time(int(timestamp * 1e9), WritePrecision.NS)  # Convert to nanoseconds
        
        # Write to InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info(f"Data stored in InfluxDB: ID=0x{payload.get('arbitration_id', 0):X}")
            
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON: {msg.payload}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main function to set up and run the MQTT client"""
    # Check if InfluxDB is properly configured
    if not INFLUXDB_TOKEN or not INFLUXDB_ORG:
        logger.error("InfluxDB not configured. Please set INFLUXDB_TOKEN and INFLUXDB_ORG in .env file")
        return
    
    try:
        # Create the bucket if it doesn't exist
        buckets_api = influx_client.buckets_api()
        bucket_name = INFLUXDB_BUCKET
        
        # Get list of bucket names
        buckets = buckets_api.find_buckets().buckets
        bucket_names = [bucket.name for bucket in buckets]
        
        # Create the bucket if it doesn't exist
        if bucket_name not in bucket_names:
            logger.info(f"Creating bucket '{bucket_name}'")
            buckets_api.create_bucket(bucket_name=bucket_name, org=INFLUXDB_ORG)
            logger.info(f"Bucket '{bucket_name}' created successfully")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists")
            
        # Create MQTT client
        client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv5)
        client.on_connect = on_connect
        client.on_message = on_message
        
        # Set TLS as per your existing code
        client.tls_set()
        
        # Set username and password if provided
        if MQTT_USERNAME and MQTT_PASSWORD:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            logger.info(f"Using authentication with username: {MQTT_USERNAME}")
        else:
            logger.warning("No MQTT credentials provided. Connection might fail.")
        
        # Connect to MQTT broker
        logger.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        # Start the MQTT loop
        client.loop_forever()
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return

if __name__ == "__main__":
    main()