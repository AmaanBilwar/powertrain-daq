#!/usr/bin/env python3
"""
Powertrain DAQ Telemetry Client Launcher

This script launches the telemetry client with appropriate environment variables.
"""

import os
import sys
import argparse
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Start the powertrain telemetry client")
    parser.add_argument("--test", action="store_true", help="Run in test mode (simulate CAN messages)")
    parser.add_argument("--websocket", action="store_true", help="Connect to WebSocket server")
    parser.add_argument("--server-uri", type=str, help="WebSocket server URI")
    parser.add_argument("--interface", type=str, help="CAN interface name")
    parser.add_argument("--sync-interval", type=int, help="Sync interval in seconds")
    args = parser.parse_args()
    
    # Set environment variables based on arguments
    env = os.environ.copy()
    
    if args.test:
        env["TEST_MODE"] = "true"
    else:
        env["TEST_MODE"] = "false"
        
    if args.websocket:
        env["WEBSOCKET_MODE"] = "true"
    else:
        env["WEBSOCKET_MODE"] = "false"
        
    if args.server_uri:
        env["SERVER_URI"] = args.server_uri
        
    if args.interface:
        env["CAN_INTERFACE"] = args.interface
        
    if args.sync_interval:
        env["SYNC_INTERVAL"] = str(args.sync_interval)
    
    # Verify Marple token is set
    if not env.get("MARPLE_ACCESS_TOKEN") and "MARPLE_ACCESS_TOKEN" not in os.environ:
        print("WARNING: MARPLE_ACCESS_TOKEN is not set. Data will be stored locally but not synced to Marple.")
    
    # Get the absolute path to the client.py script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    client_path = os.path.join(script_dir, "testing", "client.py")
    
    # Check if client.py exists
    if not os.path.exists(client_path):
        print(f"Error: Client script not found at {client_path}")
        sys.exit(1)
    
    # Print configuration
    print("\n=== Telemetry Client Configuration ===")
    print(f"Test Mode: {env.get('TEST_MODE', 'false')}")
    print(f"WebSocket Mode: {env.get('WEBSOCKET_MODE', 'false')}")
    print(f"CAN Interface: {env.get('CAN_INTERFACE', 'can0')}")
    print(f"Server URI: {env.get('SERVER_URI', 'ws://127.0.0.1:8000/ws')}")
    print(f"Sync Interval: {env.get('SYNC_INTERVAL', '300')} seconds")
    print(f"Marple Token: {'Set' if env.get('MARPLE_ACCESS_TOKEN') else 'Not Set'}")
    print("=======================================\n")
    
    # Launch the client script
    try:
        subprocess.run([sys.executable, client_path], env=env)
    except KeyboardInterrupt:
        print("\nTelemetry client stopped by user.")
    except Exception as e:
        print(f"Error running telemetry client: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 