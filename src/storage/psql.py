import psycopg2
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class DatabaseManager:
    def __init__(self, configuration: Dict = None):
        """Initialize DatabaseManager with configuration from config.yaml.
        
        Args:
            configuration: Configuration dictionary containing database settings
        """
        if configuration is None:
            raise ValueError("Configuration must be provided")
        
        self.db_config = configuration.get('database', {})
        if not self.db_config:
            raise ValueError("Database configuration not found in config")
            
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        if not self.conn:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def get_latest_data(self, required_vars: List[str], last_timestamp: Optional[datetime] = None) -> Dict:
        """Fetch latest data from database"""
        try:
            self.connect()
            # Format column names with proper quotes
            columns = ', '.join(f'"{var}"' for var in required_vars)
            
            # Build query based on whether we have a last timestamp
            if last_timestamp:
                query = """
                SELECT "timestamp", {}
                FROM process_data
                WHERE "timestamp" > %s
                ORDER BY "timestamp" DESC
                LIMIT 1;
                """.format(columns)
                self.cursor.execute(query, (last_timestamp,))
            else:
                # If no timestamp, get the latest row
                query = """
                SELECT "timestamp", {}
                FROM process_data
                ORDER BY "timestamp" DESC
                LIMIT 1;
                """.format(columns)
                self.cursor.execute(query)

            columns = [desc[0] for desc in self.cursor.description]  # Get column names
            row = self.cursor.fetchone()
            
            if not row:
                raise Exception("No data found in database")
                
            # Convert to dictionary
            data = dict(zip(columns, row))
            last_timestamp = data.pop('timestamp')  # Remove and get timestamp
            
            return {'timestamp': last_timestamp, 'data': data}

        except Exception as e:
            raise Exception(f"Database error: {str(e)}")
        finally:
            self.disconnect()

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()