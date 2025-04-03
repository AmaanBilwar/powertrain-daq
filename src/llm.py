import os
import json
import cantools
from openai import OpenAI
from dotenv import load_dotenv
import random
import time
from datetime import datetime

load_dotenv()

client = OpenAI(
    base_url="https://api.studio.nebius.com/v1/",
    api_key=os.getenv("NEBIUS_API_KEY"),
)

def read_dbc_files(file_paths):
    """
    Read DBC files and parse them with cantools library.
    
    Args:
        file_paths (list): List of paths to DBC files
    
    Returns:
        dict: Dictionary with file metadata and message definitions
    """
    dbc_metadata = {}
    
    for file_path in file_paths:
        try:
            # Parse DBC file with cantools
            db = cantools.database.load_file(file_path)
            
            # Extract message definitions
            messages = {}
            for message in db.messages:
                signals = {}
                for signal in message.signals:
                    signals[signal.name] = {
                        'min': signal.minimum if signal.minimum is not None else 0,
                        'max': signal.maximum if signal.maximum is not None else 100,
                        'unit': signal.unit if signal.unit else '',
                        'description': signal.comment if signal.comment else ''
                    }
                
                messages[message.name] = {
                    'id': hex(message.frame_id),
                    'signals': signals,
                    'description': message.comment if message.comment else ''
                }
            
            dbc_metadata[os.path.basename(file_path)] = {
                'messages': messages
            }
            
            print(f"Successfully parsed {file_path}")
        except FileNotFoundError:
            print(f"Error: File {file_path} not found")
        except Exception as e:
            print(f"Error parsing {file_path}: {str(e)}")
    
    return dbc_metadata

def generate_synthetic_can_data(dbc_metadata, num_messages=100):
    """
    Generate synthetic CAN messages based on DBC file metadata
    
    Args:
        dbc_metadata (dict): Dictionary with DBC file metadata
        num_messages (int): Number of messages to generate
    
    Returns:
        list: List of synthetic CAN messages
    """
    synthetic_data = []
    message_types = []
    
    # Flatten all message types from all DBC files
    for dbc_file, content in dbc_metadata.items():
        for msg_name, msg_info in content['messages'].items():
            message_types.append({
                'dbc_file': dbc_file,
                'name': msg_name,
                'id': msg_info['id'],
                'signals': msg_info['signals']
            })
    
    # Generate random timestamps within the last hour
    current_time = time.time()
    start_time = current_time - 3600  # 1 hour ago
    
    for _ in range(num_messages):
        # Pick a random message type
        if not message_types:
            continue
        
        message_type = random.choice(message_types)
        
        # Generate random values for each signal
        signals_data = {}
        for signal_name, signal_info in message_type['signals'].items():
            min_val = signal_info.get('min', 0)
            max_val = signal_info.get('max', 100)
            
            # Generate random value appropriate for the signal
            if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                if isinstance(min_val, int) and isinstance(max_val, int):
                    value = random.randint(min_val, max_val)
                else:
                    value = random.uniform(min_val, max_val)
                    value = round(value, 2)  # Round to 2 decimal places
            else:
                value = random.randint(0, 100)  # Default range if min/max not numeric
            
            signals_data[signal_name] = value
        
        # Generate a random timestamp
        timestamp = random.uniform(start_time, current_time)
        formatted_time = datetime.fromtimestamp(timestamp).isoformat()
        
        # Create the message
        can_message = {
            'timestamp': formatted_time,
            'can_id': message_type['id'],
            'name': message_type['name'],
            'signals': signals_data,
            'dbc_file': message_type['dbc_file']
        }
        
        synthetic_data.append(can_message)
    
    return synthetic_data

# Define the DBC file paths with proper path handling
dbc_files = [
    os.path.join("dbc_files", "EV3_Vehicle_Bus.dbc"),
    os.path.join("dbc_files", "RMS.dbc")
]

# Read and parse the DBC files
dbc_metadata = read_dbc_files(dbc_files)

if not dbc_metadata:
    # If DBC parsing failed, use LLM to generate synthetic data
    print("DBC parsing failed. Using LLM for synthetic data generation...")
    
    prompt = f"""
    Generate 20 synthetic CAN messages for an electric vehicle system. 
    Each message should include:
    1. A CAN ID (in hex format like 0x123)
    2. A timestamp in ISO format
    3. A message name
    4. Signal values with appropriate ranges for an electric vehicle

    Format the output as a JSON array of objects. Each object should have this structure:
    {{
        "timestamp": "2023-04-01T12:34:56.789",
        "can_id": "0x123",
        "name": "MOTOR_TEMPERATURE",
        "signals": {{
            "motor_temp": 65.5,
            "controller_temp": 48.2
        }}
    }}

    Include common EV signals like battery voltage, motor temperature, vehicle speed, etc.
    """

    completion = client.chat.completions.create(
        model="meta-llama/Llama-3.3-70B-Instruct",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1500,
        temperature=0.6,
    )

    try:
        # Try to parse the response as JSON
        result = completion.choices[0].message.content
        
        # Extract JSON part from the response
        import re
        json_match = re.search(r'\[[\s\S]*\]', result)
        if json_match:
            synthetic_data = json.loads(json_match.group(0))
        else:
            raise ValueError("Could not find JSON array in response")
            
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing LLM output: {e}")
        print("Raw output:", completion.choices[0].message.content)
        # Fallback to a simple structure
        synthetic_data = []
else:
    # Generate synthetic data based on DBC metadata
    synthetic_data = generate_synthetic_can_data(dbc_metadata, num_messages=50)

# Save the synthetic data to both JSON and MQTT-friendly formats
def save_data_to_file(data, file_path, format_type='json'):
    """
    Save data to a file
    
    Args:
        data: The data to save
        file_path: Path to output file
        format_type: 'json' or 'mqtt'
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if format_type == 'json':
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=2)
        elif format_type == 'mqtt':
            # Format suitable for MQTT publishing
            with open(file_path, 'w') as file:
                for message in data:
                    # Format as topic and payload
                    topic = f"can/{message['can_id']}"
                    payload = json.dumps({
                        'timestamp': message['timestamp'],
                        'name': message.get('name', ''),
                        'signals': message['signals']
                    })
                    file.write(f"{topic}|{payload}\n")
        
        print(f"Successfully saved data to {file_path}")
        return True
    except Exception as e:
        print(f"Error saving data to {file_path}: {str(e)}")
        return False

# Save as JSON
json_file = "generated_can_messages.json"
save_data_to_file(synthetic_data, json_file, 'json')

# Save in MQTT-friendly format
mqtt_file = "mqtt_can_messages.txt"
save_data_to_file(synthetic_data, mqtt_file, 'mqtt')

print(f"Generated {len(synthetic_data)} synthetic CAN messages")
print(f"Saved to {json_file} and {mqtt_file}")