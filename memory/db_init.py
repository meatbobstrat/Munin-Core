import os
import sqlite3

# Define the path for the SQLite database file (e.g., "munin.db" in this directory)
DB_FILENAME = "munin.db"
DB_PATH = os.path.join(os.path.dirname(__file__), DB_FILENAME)

def init_db(db_path: str = DB_PATH) -> None:
    """
    Initialize the SQLite database for security events. Creates the database file 
    (if it doesn't exist) and ensures the 'events' table is present with the required schema.
    
    Schema (events table):
      - id: INTEGER PRIMARY KEY AUTOINCREMENT
      - timestamp: TEXT (UTC timestamp of ingestion)
      - echo_time: TEXT (formatted as DD.HH.MM from ET 0.0.0)
      - source_file: TEXT (name of the ingested log file)
      - summary: TEXT (LLM-generated summary of the event)
      - raw_excerpt: TEXT (short excerpt of the raw log content)
    """
    conn = None
    try:
        # Connect to the SQLite database (this will create the file if it doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Create the events table with the specified schema, if it doesn't already exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                echo_time TEXT,
                source_file TEXT,
                summary TEXT,
                raw_excerpt TEXT
            );
        """)
        conn.commit()  # Commit the changes to ensure the table is created
    except Exception as e:
        # Basic error handling: print or log the error
        print(f"Error initializing database: {e}")
    finally:
        # Ensure the database connection is closed properly
        if conn:
            conn.close()

# If this module is run as a script, perform initialization.
if __name__ == "__main__":
    init_db()
    print(f"Database initialized (path: {DB_PATH}).")
