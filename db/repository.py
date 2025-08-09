import sqlite3
import os
from datetime import datetime
from utils.logger import logger
from config import DB_DIR, SCHEMA_SQL

class Repository:
    def __init__(self, db_name="twitter_profiles.db"):
        os.makedirs(DB_DIR, exist_ok=True)
        self.db_path = os.path.join(DB_DIR, db_name)
        self._initialize_db()

    def _initialize_db(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            with open(SCHEMA_SQL, 'r') as f:
                schema_sql = f.read()
            
            # Split the schema into individual statements
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            for statement in statements:
                try:
                    cursor.execute(statement + ';')
                except sqlite3.OperationalError as e:
                    # Ignore "index already exists" errors
                    if "already exists" in str(e):
                        logger.debug(f"Skipping existing database object: {e}")
                    else:
                        raise
            
            conn.commit()
            logger.log(f"Database initialized at {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _execute_query(self, query, params=(), fetch_one=False, fetch_all=False):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            if fetch_one:
                return cursor.fetchone()
            if fetch_all:
                return cursor.fetchall()
            return None
        except sqlite3.Error as e:
            logger.error(f"Database query error: {e} - Query: {query} - Params: {params}")
            raise
        finally:
            if conn:
                conn.close()

    def record_new_profile(self, twitter_handle, notion_page_id, source_username):
        """
        Records a new profile or updates an existing one.
        Uses the two-table structure: processed_profiles and source_relationships
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current timestamp
            now = datetime.now().isoformat()
            
            # Check if profile already exists
            cursor.execute("SELECT twitter_handle FROM processed_profiles WHERE twitter_handle = ?", (twitter_handle,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing profile
                cursor.execute("""
                    UPDATE processed_profiles 
                    SET last_updated_date = ?, notion_page_id = ?
                    WHERE twitter_handle = ?
                """, (now, notion_page_id, twitter_handle))
            else:
                # Insert new profile
                cursor.execute("""
                    INSERT INTO processed_profiles 
                    (twitter_handle, first_discovered_date, last_updated_date, notion_page_id, category)
                    VALUES (?, ?, ?, ?, NULL)
                """, (twitter_handle, now, now, notion_page_id))
            
            # Record source relationship
            cursor.execute("""
                INSERT OR IGNORE INTO source_relationships 
                (twitter_handle, discovered_by_handle, discovery_date)
                VALUES (?, ?, ?)
            """, (twitter_handle, source_username, now))
            
            conn.commit()
            logger.debug(f"Recorded/updated profile {twitter_handle} for source {source_username}")
        except Exception as e:
            logger.error(f"Failed to record new profile {twitter_handle}: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_processed_profile(self, twitter_handle, source_username):
        """
        Gets a processed profile checking both the profile and source relationship
        """
        query = """
        SELECT p.twitter_handle, p.notion_page_id, p.last_updated_date
        FROM processed_profiles p
        INNER JOIN source_relationships sr ON p.twitter_handle = sr.twitter_handle
        WHERE p.twitter_handle = ? AND sr.discovered_by_handle = ?
        """
        try:
            result = self._execute_query(query, (twitter_handle, source_username), fetch_one=True)
            return {
                "twitter_handle": result[0],
                "notion_page_id": result[1],
                "processed_at": result[2]
            } if result else None
        except Exception as e:
            logger.error(f"Failed to get processed profile {twitter_handle}: {e}")
            return None

    def find_by_handle(self, twitter_handle):
        """
        Find a profile by Twitter handle (globally, like JavaScript)
        """
        query = "SELECT * FROM processed_profiles WHERE twitter_handle = ?"
        try:
            result = self._execute_query(query, (twitter_handle,), fetch_one=True)
            if result:
                return {
                    "twitter_handle": result[0],
                    "first_discovered_date": result[1],
                    "last_updated_date": result[2],
                    "notion_page_id": result[3],
                    "category": result[4],
                    "created_at": result[5]
                }
            return None
        except Exception as e:
            logger.error(f"Failed to find profile by handle {twitter_handle}: {e}")
            return None

    def update_last_seen(self, twitter_handle):
        """
        Update the last_updated_date for a profile
        """
        now = datetime.now().isoformat()
        query = "UPDATE processed_profiles SET last_updated_date = ? WHERE twitter_handle = ?"
        try:
            self._execute_query(query, (now, twitter_handle))
        except Exception as e:
            logger.error(f"Failed to update last seen for {twitter_handle}: {e}")

    def add_source_relationship(self, twitter_handle, source_username):
        """
        Add a new source relationship (or ignore if already exists)
        """
        now = datetime.now().isoformat()
        query = """
        INSERT OR IGNORE INTO source_relationships 
        (twitter_handle, discovered_by_handle, discovery_date)
        VALUES (?, ?, ?)
        """
        try:
            self._execute_query(query, (twitter_handle, source_username, now))
        except Exception as e:
            logger.error(f"Failed to add source relationship for {twitter_handle}: {e}")

    def get_sources_for_profile(self, twitter_handle):
        """
        Get all sources that discovered this profile
        """
        query = """
        SELECT discovered_by_handle, discovery_date 
        FROM source_relationships 
        WHERE twitter_handle = ?
        ORDER BY discovery_date
        """
        try:
            results = self._execute_query(query, (twitter_handle,), fetch_all=True)
            return [
                {
                    "discovered_by": row[0],
                    "discovery_date": row[1]
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to get sources for profile {twitter_handle}: {e}")
            return []

# Initialize a global repository instance
repository = Repository()