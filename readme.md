# DAQ Powertrain Software

A comprehensive toolkit for processing, decoding, and analyzing CAN (Controller Area Network) bus data from electric vehicles.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Usage](#usage)
- [Data Processing Pipeline](#data-processing-pipeline)
- [Contributing](#contributing)

## Overview
DAQ Powertrain Software is designed to decode and process CAN bus messages from electric vehicles using DBC (Database CAN) files. It supports parsing raw CAN logs, decoding messages according to DBC specifications, and generating synthetic CAN data for testing purposes.

## Features
- **CAN Message Decoding**: Parse raw CAN logs and decode messages using multiple DBC files
- **Synthetic Data Generation**: Generate realistic CAN messages for testing using LLM (Large Language Model) capabilities
- **MQTT Integration**: Support for MQTT protocol for real-time data transmission
- **Data Collection**: Tools for collecting and storing CAN data
- **Multiple DBC Support**: Process signals from various vehicle subsystems using multiple DBC files

## Project Structure
- `/dbc_files` - Contains DBC files that define the CAN message format
  - `EV3_Vehicle_Bus.dbc` - Main vehicle bus message definitions
  - `RMS.dbc` - RMS (Rinehart Motion Systems) controller message definitions
- `/raw_can_files` - Raw CAN log files for processing
- `/src` - Source code
  - `can_decoder.py` - Core functionality for decoding CAN messages
  - `llm.py` - LLM-based synthetic data generation
  - `data_collection.py` - Data collection utilities
  - `mqtt.py` - MQTT communication module
  - `app.py` - Main application entry point

## Setup
1. Clone the repository
2. Install the required dependencies:
`pip install -r requirements.txt`
3. Set up environment variables in `.env` file:
`NEBIUS_API_KEY=your_api_key_here`

## Usage
### Decoding CAN Messages
To decode raw CAN messages from a log file:
`python can_decoder.py`
This will:
1. Load DBC files from the `dbc_files` directory
2. Parse raw CAN log from `raw_can_files/2-13-25.txt`
3. Decode messages and save to `decoded_can_messages.csv`

### Generating Synthetic Data
To generate synthetic CAN data for testing:
`python llm.py`

This will create:
- `generated_can_messages.json` - JSON format of synthetic CAN messages
- `mqtt_can_messages.txt` - MQTT-ready format of synthetic messages

## Data Processing Pipeline
1. **Raw Data Collection** - CAN messages are collected from the vehicle
2. **Decoding** - Raw messages are decoded using DBC specifications
3. **Analysis** - Decoded data can be analyzed or transmitted via MQTT
4. **Testing** - Synthetic data can be generated for system testing

## Contributing
Please refer to the project's issue tracker for current development tasks and the `src/to-do.md` file for planned features.