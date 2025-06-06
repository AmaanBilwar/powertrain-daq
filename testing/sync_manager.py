import os
import time
import asyncio
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from network_utils import (
    check_internet_connection,
    check_marple_connection,
    get_last_synced_id,
    update_last_synced_id,
    get_new_messages_from_db,
    convert_messages_to_dataframe,
    upload_to_marple
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'can_messages.db')
DEFAULT_SYNC_STATUS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'last_synced_id.txt')
DEFAULT_SYNC_INTERVAL = 60  # 1 minute
    
class SyncManager:
    def __init__(
        self, 
        db_path=DEFAULT_DB_PATH, 
        sync_status_path=DEFAULT_SYNC_STATUS_PATH,
        sync_interval=DEFAULT_SYNC_INTERVAL,
        batch_size=1000,
        marple_folder="/powertrain-daq/can_data"
    ):
        self.db_path = db_path
        self.sync_status_path = sync_status_path
        self.sync_interval = sync_interval
        self.batch_size = batch_size
        self.marple_folder = marple_folder
        self.running = False
        self.token = os.getenv("MARPLE_ACCESS_TOKEN")
        
        # Ensure the status file directory exists
        os.makedirs(os.path.dirname(self.sync_status_path), exist_ok=True)

    def connect_db(self):
        """Connect to the SQLite database"""
        return sqlite3.connect(self.db_path)

    async def sync_loop(self):
        """Main sync loop that periodically checks for internet and syncs data"""
        self.running = True
        logger.info("Starting Marple sync manager")
        
        while self.running:
            try:
                # Check for internet connectivity
                if check_internet_connection():
                    logger.info("Internet connection available, checking Marple access")
                    
                    # Check if Marple API is accessible
                    if check_marple_connection(self.token):
                        logger.info("Marple connection successful, syncing data")
                        
                        # Connect to database
                        conn = self.connect_db()
                        
                        # Get last synced message ID
                        last_id = get_last_synced_id(self.sync_status_path)
                        logger.info(f"Last synced message ID: {last_id}")
                        
                        # Get new messages
                        messages = get_new_messages_from_db(conn, last_id, self.batch_size)
                        
                        if messages:
                            logger.info(f"Found {len(messages)} new messages to sync")
                            
                            # Convert messages to DataFrame
                            df = convert_messages_to_dataframe(messages)
                            
                            # Upload to Marple
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            source_name = f"can_data_{timestamp}"
                            source_id = upload_to_marple(self.token, df, source_name, self.marple_folder)
                            
                            if source_id:
                                # Update last synced message ID
                                max_id = max(msg['id'] for msg in messages)
                                update_last_synced_id(self.sync_status_path, max_id)
                                logger.info(f"Successfully synced messages up to ID {max_id}")
                        else:
                            logger.info("No new messages to sync")
                        
                        # Close database connection
                        conn.close()
                    else:
                        logger.warning("Marple API is not accessible")
                else:
                    logger.warning("No internet connection available")
            
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
            
            # Wait for next sync interval
            logger.info(f"Waiting {self.sync_interval} seconds until next sync attempt")
            await asyncio.sleep(self.sync_interval)
    
    def stop(self):
        """Stop the sync loop"""
        logger.info("Stopping sync manager")
        self.running = False


async def start_sync_manager(
    db_path=DEFAULT_DB_PATH,
    sync_status_path=DEFAULT_SYNC_STATUS_PATH,
    sync_interval=DEFAULT_SYNC_INTERVAL
):
    """Start the sync manager in the background"""
    sync_manager = SyncManager(db_path, sync_status_path, sync_interval)
    asyncio.create_task(sync_manager.sync_loop())
    return sync_manager


if __name__ == "__main__":
    # For testing the sync manager independently
    async def main():
        sync_manager = SyncManager()
        try:
            await sync_manager.sync_loop()
        except KeyboardInterrupt:
            sync_manager.stop()
    
    asyncio.run(main()) 