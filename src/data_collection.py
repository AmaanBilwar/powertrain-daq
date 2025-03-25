import can
import time
import sqlite3

class CANDataCollector:
    def __init__(self, can_channel='can0', bitrate=500000):
        """
        Initialize CAN bus connection and local SQLite database
        
        :param can_channel: SocketCAN interface name
        :param bitrate: CAN bus communication speed
        """
        # CAN Bus Configuration
        try:
            # Configure CAN interface
            print(f"Configuring CAN interface {can_channel} at {bitrate} bps")
            
            # Note: You might need to run these commands in terminal beforehand:
            # sudo ip link set can0 type can bitrate 500000
            # sudo ip link set up can0
            
            self.bus = can.interface.Bus(channel=can_channel, bustype='socketcan')
        except Exception as e:
            print(f"CAN Bus Initialization Error: {e}")
            raise
        
        # SQLite Database Setup
        self.conn = sqlite3.connect('can_data_log.db')
        self.cursor = self.conn.cursor()
        
        # Create table to store CAN messages
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS can_messages (
                timestamp REAL,
                arbitration_id INTEGER,
                data BLOB,
                dlc INTEGER
            )
        ''')
        self.conn.commit()
    
    def collect_data(self, duration=60):
        """
        Collect CAN bus data for specified duration
        
        :param duration: Data collection time in seconds
        """
        print(f"Starting CAN data collection for {duration} seconds")
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration:
                # Receive CAN message
                message = self.bus.recv(1.0)  # 1 second timeout
                
                if message:
                    # Log detailed message information
                    print(f"Received Message - ID: {message.arbitration_id}, "
                          f"Data: {message.data}, "
                          f"DLC: {message.dlc}")
                    
                    # Store message in SQLite
                    self.cursor.execute('''
                        INSERT INTO can_messages 
                        (timestamp, arbitration_id, data, dlc) 
                        VALUES (?, ?, ?, ?)
                    ''', (
                        time.time(), 
                        message.arbitration_id, 
                        message.data, 
                        message.dlc
                    ))
                    
                    # Commit every message to ensure data is saved
                    self.conn.commit()
        
        except KeyboardInterrupt:
            print("Data collection stopped by user")
        
        except Exception as e:
            print(f"Error during data collection: {e}")
        
        finally:
            # Close CAN bus and database connections
            self.bus.shutdown()
            self.conn.close()
    
    def retrieve_logged_data(self):
        """
        Retrieve and print logged CAN messages
        """
        self.cursor.execute('SELECT * FROM can_messages')
        messages = self.cursor.fetchall()
        
        print("\nLogged CAN Messages:")
        for msg in messages:
            print(f"Timestamp: {msg[0]}, "
                  f"Arbitration ID: {msg[1]}, "
                  f"Data: {msg[2]}, "
                  f"DLC: {msg[3]}")

# Example Usage
def main():
    try:
        # Create CAN data collector
        collector = CANDataCollector(
            can_channel='can0',  # Ensure this matches your setup
            bitrate=500000       # Adjust to your vehicle's CAN bus speed
        )
        
        # Collect data for 60 seconds
        collector.collect_data(duration=60)
        
        # Retrieve and display logged data
        collector.retrieve_logged_data()
    
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()