import can
import time 


def can_messages():
# Configure the CAN interface
    can.interface.Bus(channel='can0', bustype='socketcan')

    # Create a CSV file to store the CAN messages
    filename = 'can_messages.csv'
    with open(filename, 'w') as file:
        file.write('Timestamp,ID,Data\n')

    # Set up a message listener
    def message_listener(msg):
        timestamp = time.time()
        with open(filename, 'a') as file:
            file.write(f"{timestamp},{msg.arbitration_id:X},{msg.data.hex()}\n")

    # Create a notifier to listen for messages
    notifier = can.Notifier(can.interface.Bus(channel='can0', bustype='socketcan'), [message_listener])

    # Run the script indefinitely
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Script terminated by user")
    finally:
        notifier.stop()

def upload_to_blob():
    pass


def main():
    pass



if __name__ == "__main__":
    main()