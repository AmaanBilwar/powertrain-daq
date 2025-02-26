# Explanation of the script 'test-app.py' under `src/`
1. Load Multiple DBC Files:
    The load_dbc_files function combines multiple DBC files into a single cantools.database.Database object.

2. Parse Raw CAN Log:
    The parse_raw_can_log function reads the raw CAN log file and extracts relevant fields (message_id, data, timestamp, etc.).

3. Decode Messages:
The decode_messages function decodes each message using the loaded DBC database. If a message ID does not exist in any of the loaded DBC files, it is skipped.

4. Save Decoded Messages:
The save_decoded_messages function writes the decoded data to a CSV file for easier analysis.

5. Main Function:
The main function orchestrates the process: loading DBC files, parsing the raw log, decoding the messages, and saving them.

# Output Format
The output CSV (decoded_can_messages.csv) will have columns:

- timestamp: The timestamp of each message.

- message_id: The hexadecimal ID of the message.

- decoded_data: A dictionary of signal names and their values.

# Notes
1. if not already done so, in this script, replpace file1.dbc and file2.dbc with the actual paths to your DBC files.

2. Replace "2-13-25.txt" with your raw CAN log file path. That is just a placeholder. 

3. Ensure that your raw log format matches what is parsed in parse_raw_can_log.

<br>

**This script should handle your requirements of processing a raw CAN log and decoding it using multiple DBC files. Let me know if you need further assistance!**