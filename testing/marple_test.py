import os
import sqlite3
import pandas as pd
from marple import Marple
from dotenv import load_dotenv
from pathlib import Path
import tempfile

# Load environment variables
load_dotenv()


def get_marple_token():
    """Get Marple access token from environment variables"""
    token = os.getenv("MARPLE_ACCESS_TOKEN")
    if not token:
        raise ValueError("MARPLE_ACCESS_TOKEN not found in environment variables")
    return token


def get_merged_signals():
    """Read and join signals with can_messages to include timestamp for Marple upload."""
    db_path = Path(__file__).parent.parent / "data" / "can_messages.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found at: {db_path}")
    conn = sqlite3.connect(str(db_path))

    # Read both tables
    can_messages_df = pd.read_sql_query("SELECT id, timestamp FROM can_messages", conn)
    signals_df = pd.read_sql_query("SELECT * FROM signals", conn)

    # Merge signals with can_messages to get timestamp for each signal
    merged = signals_df.merge(
        can_messages_df,
        left_on="message_id",
        right_on="id",
        suffixes=("_signal", "_can"),
    )

    # Select and rename columns for Marple
    marple_df = merged.rename(
        columns={
            "timestamp": "timestamp",
            "signal_name": "signal",
            "value": "value",
            "unit": "unit",
        }
    )
    marple_df = marple_df[["timestamp", "signal", "value", "unit", "message_id"]]

    conn.close()
    return marple_df


def get_merged_signals_wide():
    """Read and join signals with can_messages to include timestamp for Marple upload, pivoted to wide format."""
    db_path = Path(__file__).parent.parent / "data" / "can_messages.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found at: {db_path}")
    conn = sqlite3.connect(str(db_path))

    # Read both tables
    can_messages_df = pd.read_sql_query("SELECT id, timestamp FROM can_messages", conn)
    signals_df = pd.read_sql_query("SELECT * FROM signals", conn)

    # Merge signals with can_messages to get timestamp for each signal
    merged = signals_df.merge(
        can_messages_df,
        left_on="message_id",
        right_on="id",
        suffixes=("_signal", "_can"),
    )

    # Select and rename columns for Marple
    marple_df = merged.rename(
        columns={"timestamp": "timestamp", "signal_name": "signal", "value": "value"}
    )
    marple_df = marple_df[["timestamp", "signal", "value"]]
    # Pivot so each signal is a column
    wide_df = marple_df.pivot(
        index="timestamp", columns="signal", values="value"
    ).reset_index()

    conn.close()
    return wide_df


def upload_to_marple(m, data, source_name, folder_path="/powertrain-daq"):
    """Upload data to Marple using CSV file"""
    import time

    try:
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file_path = temp_file.name
            data.to_csv(temp_file_path, index=False)
        # Now the file is closed, safe to upload
        source_id = m.upload_data_file(
            temp_file_path,
            folder_path,
            metadata={
                "source": source_name,
                "description": f"Wide format signals with timestamp for Marple upload",
            },
        )
        print(f"Successfully uploaded {source_name} to Marple")
        # Import the data
        path = f"{folder_path}/{os.path.basename(temp_file_path)}"
        m.post(
            "/library/file/import",
            json={
                "path": path,
                "plugin": "csv",
                "config": {"common": [{"name": "time_column", "value": "timestamp"}]},
            },
        )
        # Check import status
        status = m.check_import_status(source_id)
        print(f"Import status for {source_name}: {status}%")
        # Clean up the temporary file
        os.unlink(temp_file_path)
        return source_id
    except Exception as e:
        print(f"Error uploading {source_name}: {str(e)}")
        return None


def main():
    try:
        # Initialize Marple client
        token = get_marple_token()
        m = Marple(token)

        # Test connection
        print("Testing Marple connection...")
        m.check_connection()
        print("Connection successful!")

        # Get wide format merged signals data
        print(
            "\nReading and merging signals with timestamps from database (wide format)..."
        )
        wide_signals = get_merged_signals_wide()
        print(f"Found {len(wide_signals)} wide signal rows")
        print("Columns:")
        print(wide_signals.columns.tolist())

        # Upload wide format signals to Marple
        print("\nUploading wide format signals to Marple...")
        wide_source_id = upload_to_marple(
            m,
            wide_signals,
            "signals_wide_with_timestamp",
            "/powertrain-daq/signals_wide",
        )

        print("\nUpload complete!")
        if wide_source_id:
            print(f"Wide format signals source ID: {wide_source_id}")

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
