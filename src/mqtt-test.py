"""
Test script for MQTT connection without requiring CAN bus hardware
"""

import time
import json
import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    if rc == 0:
        print(f"Connected to MQTT Broker with result code {rc}")
    else:
        print(f"Failed to connect to MQTT Broker. Return code {rc}")

def on_publish(client, userdata, mid):
    """Callback when a message is published"""
    print(f"Message {mid} published successfully")

def simulate_can_message():
    """Generate a simulated CAN message"""
    return {
        'timestamp': time.time(),
        'arbitration_id': random.randint(0x100, 0x7FF),  # Random CAN ID
        'data': [random.randint(0, 255) for _ in range(8)],  # Random 8 bytes
        'dlc': 8
    }

def main():
    try:
        # MQTT Client Configuration
        client_id = f"fsae_mqtt_test_{int(time.time())}"
        mqtt_broker = os.getenv('MQTT_BROKER', 'broker.hivemq.com')
        mqtt_port = int(os.getenv('MQTT_PORT', 8883))
        mqtt_topic = os.getenv('MQTT_TOPIC', 'fsae/telemetry')
        
        print(f"Connecting to {mqtt_broker}:{mqtt_port} as {client_id}")
        mqtt_client = mqtt.Client(client_id=client_id)
        
        # Set callbacks
        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish
        
        # Get credentials from environment variables
        username = os.getenv('MQTT_USERNAME')
        password = os.getenv('MQTT_PASSWORD')
        
        # HiveMQ Cloud requires TLS/SSL
        mqtt_client.tls_set()
        
        # HiveMQ Cloud requires authentication
        if username and password:
            mqtt_client.username_pw_set(username, password)
            print(f"Using authentication with username: {username}")
        else:
            print("Warning: MQTT credentials not provided. Connection might fail.")
        
        # Connect to broker
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)
        
        # Start the loop
        mqtt_client.loop_start()
        
        # Give time for connection to establish
        time.sleep(1)
        
        # Number of test messages to send
        msg_count = int(os.getenv('MQTT_TEST_COUNT', 10))
        interval = float(os.getenv('MQTT_TEST_INTERVAL', 1.0))
        
        print(f"Publishing {msg_count} test messages at {interval}s interval")
        
        # Send test messages
        for i in range(msg_count):
            message = simulate_can_message()
            json_payload = json.dumps(message)
            
            result = mqtt_client.publish(mqtt_topic, json_payload)
            status = result[0]
            
            if status == 0:
                print(f"Message {i+1}/{msg_count} sent: {message['arbitration_id']}")
            else:
                print(f"Failed to send message {i+1}/{msg_count}. Error code: {status}")
            
            time.sleep(interval)
        
        # Give time for the last publish to complete
        time.sleep(1)
        
        # Disconnect
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("MQTT test completed")
    
    except Exception as e:
        print(f"Error in test execution: {e}")

if __name__ == "__main__":
    main()