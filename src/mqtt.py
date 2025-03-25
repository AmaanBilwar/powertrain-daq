'''
here we picked a cloud broker hivemq, we can also pick a local broker like mosquitto
this decision is based on the fact that hivemq is a cloud broker and it has a free tier avalailable to use
'''

import can
import time
import sqlite3
import json
import paho.mqtt.client as mqtt

class CANDataMQTTTransmitter:
    def __init__(self, 
                 can_channel='can0', 
                 bitrate=500000, 
                 mqtt_broker='localhost', 
                 mqtt_port=1883,
                 mqtt_topic='fsae/telemetry'):
        """
        Initialize CAN bus and MQTT client
        
        :param can_channel: SocketCAN interface name
        :param bitrate: CAN bus communication speed
        :param mqtt_broker: MQTT broker address
        :param mqtt_port: MQTT broker port
        :param mqtt_topic: MQTT topic for publishing
        """
        # CAN Bus Configuration
        try:
            self.bus = can.interface.Bus(channel=can_channel, bustype='socketcan')
        except Exception as e:
            print(f"CAN Bus Initialization Error: {e}")
            raise
        
        # MQTT Client Configuration
        self.mqtt_client = mqtt.Client(client_id=f"fsae_can_transmitter_{time.time()}")
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        
        # SQLite Database for local logging and backup
        self.conn = sqlite3.connect('can_mqtt_log.db')
        self.cursor = self.conn.cursor()
        
        # Create table to store CAN messages and transmission status
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS can_mqtt_messages (
                timestamp REAL,
                arbitration_id INTEGER,
                data BLOB,
                dlc INTEGER,
                mqtt_transmitted INTEGER DEFAULT 0
            )
        ''')
        self.conn.commit()
    
    def connect_mqtt(self, username=None, password=None):
        """
        Connect to MQTT broker with optional authentication
        
        :param username: MQTT broker username
        :param password: MQTT broker password
        """
        try:
            # Optional authentication
            if username and password:
                self.mqtt_client.username_pw_set(username, password)
            
            # Connect to broker
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            
            # Start the loop
            self.mqtt_client.loop_start()
            print(f"Connected to MQTT Broker: {self.mqtt_broker}")
        
        except Exception as e:
            print(f"MQTT Connection Error: {e}")
            raise
    
    def transmit_can_messages(self, duration=60):
        """
        Collect CAN messages and transmit via MQTT
        
        :param duration: Data collection time in seconds
        """
        print(f"Starting CAN message collection and MQTT transmission for {duration} seconds")
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration:
                # Receive CAN message
                message = self.bus.recv(1.0)  # 1 second timeout
                
                if message:
                    # Prepare message payload
                    payload = {
                        'timestamp': time.time(),
                        'arbitration_id': message.arbitration_id,
                        'data': list(message.data),  # Convert bytes to list for JSON serialization
                        'dlc': message.dlc
                    }
                    
                    # Convert to JSON
                    json_payload = json.dumps(payload)
                    
                    # Publish to MQTT topic
                    try:
                        self.mqtt_client.publish(self.mqtt_topic, json_payload)
                        transmission_status = 1
                        print(f"Transmitted Message - ID: {message.arbitration_id}")
                    except Exception as publish_error:
                        print(f"MQTT Transmission Error: {publish_error}")
                        transmission_status = 0
                    
                    # Store in SQLite with transmission status
                    self.cursor.execute('''
                        INSERT INTO can_mqtt_messages 
                        (timestamp, arbitration_id, data, dlc, mqtt_transmitted) 
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        time.time(), 
                        message.arbitration_id, 
                        message.data, 
                        message.dlc,
                        transmission_status
                    ))
                    
                    # Commit every message
                    self.conn.commit()
        
        except KeyboardInterrupt:
            print("Data transmission stopped by user")
        
        except Exception as e:
            print(f"Error during CAN message transmission: {e}")
        
        finally:
            # Cleanup
            self.bus.shutdown()
            self.mqtt_client.loop_stop()
            self.conn.close()
    
    def get_transmission_log(self):
        """
        Retrieve transmission logs
        """
        self.cursor.execute('''
            SELECT * FROM can_mqtt_messages 
            WHERE mqtt_transmitted = 1
        ''')
        successful_transmissions = self.cursor.fetchall()
        
        print("\nSuccessful MQTT Transmissions:")
        for msg in successful_transmissions:
            print(f"Timestamp: {msg[0]}, "
                  f"Arbitration ID: {msg[1]}, "
                  f"Data: {msg[2]}, "
                  f"DLC: {msg[3]}")

# Example Usage
def main():
    try:
        # Create MQTT transmitter
        transmitter = CANDataMQTTTransmitter(
            can_channel='can0',     # Adjust to your CAN interface
            bitrate=500000,         # Adjust to your CAN bus speed
            mqtt_broker='localhost',# Replace with your MQTT broker address
            mqtt_port=1883,         # Default MQTT port
            mqtt_topic='fsae/telemetry'
        )
        
        # Connect to MQTT (optional authentication)
        transmitter.connect_mqtt(
            # Uncomment and add credentials if required
            # username='your_username', 
            # password='your_password'
        )
        
        # Transmit CAN messages for 60 seconds
        transmitter.transmit_can_messages(duration=60)
        
        # Print transmission log
        transmitter.get_transmission_log()
    
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()