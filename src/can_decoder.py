import cantools
import csv
from datetime import datetime

def load_dbc_files(dbc_files):
    """
    Load multiple DBC files into a single database.
    """
    db = cantools.database.Database()
    for dbc_file in dbc_files:
        db.add_dbc_file(dbc_file)
    return db

def parse_raw_can_log(file_path):
    """
    Parse a raw CAN log file and extract CAN messages.
    """
    messages = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split()
            if len(parts) < 10 or parts[2] == "ErrorFrame":
                continue  # Skip invalid or error frames
            
            try:
                # Extract fields from the raw log
                channel = parts[0]
                message_id = int(parts[1])
                dlc = int(parts[2])
                data = [int(byte) for byte in parts[3:3+dlc]]
                timestamp = float(parts[-2])
                
                # Append parsed message
                messages.append({
                    "channel": channel,
                    "id": message_id,
                    "dlc": dlc,
                    "data": data,
                    "timestamp": timestamp
                })
            except ValueError:
                continue  # Skip lines with parsing errors

    return messages

def decode_messages(messages, db):
    """
    Decode CAN messages using the loaded DBC database.
    """
    decoded_messages = []
    for msg in messages:
        try:
            decoded = db.decode_message(msg["id"], bytes(msg["data"]))
            # Convert timestamp to datetime and format it 
            timestamp = datetime.fromtimestamp(msg["timestamp"]).strftime('%Y-%m-%d %H:%M:%S.%f')
            decoded_messages.append({
                "timestamp": timestamp,
                "message_id": f"0x{msg['id']:X}",
                "decoded_data": decoded
            })
        except KeyError:
            # Message ID not found in DBC; skip decoding
            continue
    return decoded_messages

def save_decoded_messages(decoded_messages, output_file):
    """
    Save decoded messages to a CSV file.
    """
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'message_id', 'decoded_data']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for msg in decoded_messages:
            writer.writerow(msg)

def main():
    # File paths
    raw_log_file = "raw_can_files/2-13-25.txt"  
    dbc_files = ["dbc_files/EV3_Vehicle_Bus.dbc", "dbc_files/RMS.dbc"]  
    output_file = "decoded_can_messages.csv"

    # Load DBC files
    print("Loading DBC files...")
    db = load_dbc_files(dbc_files)

    # Parse raw CAN log file
    print("Parsing raw CAN log...")
    messages = parse_raw_can_log(raw_log_file)

    # Decode messages using the loaded DBC database
    print("Decoding messages...")
    decoded_messages = decode_messages(messages, db)

    # Save decoded messages to a CSV file
    print(f"Saving decoded messages to {output_file}...")
    save_decoded_messages(decoded_messages, output_file)
    
    print("Done!")

if __name__ == "__main__":
    main()
