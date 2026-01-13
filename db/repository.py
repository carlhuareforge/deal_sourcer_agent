import sqlite3
import os
from datetime import datetime
from utils.logger import logger
from config import DB_DIR, SCHEMA_SQL

class Repository:
    def __init__(self, db_name="twitter_profiles.db"):
        os.makedirs(DB_DIR, exist_ok=True)
        self.db_path = os.path.join(DB_DIR, db_name)
        self._initialized = False

    @staticmethod
    def _normalize_handle(value):
        if value is None:
            return None
        if not isinstance(value, str):
            return str(value).strip().lstrip("@").lower()
        return value.strip().lstrip("@").lower()

    def _ensure_initialized(self):
        """Ensure database is initialized before any operation"""
        if not self._initialized:
            self._initialize_db()
            self._initialized = True
    
    def _initialize_db(self):
        # Check if database already exists
        db_exists = os.path.exists(self.db_path)
        
        if db_exists:
            # Database exists, just verify it's valid
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                # Quick check that tables exist
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_profiles'")
                if cursor.fetchone():
                    logger.debug(f"Using existing database at {self.db_path}")
                    conn.close()
                    return
                conn.close()
                # Tables don't exist, need to create them
            except sqlite3.Error:
                # Database is corrupted, will recreate below
                pass
        
        # Only create/initialize if doesn't exist or is invalid
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
            if not db_exists:
                logger.log(f"Created new database at {self.db_path}")
            else:
                logger.log(f"Initialized database schema at {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _execute_query(self, query, params=(), fetch_one=False, fetch_all=False):
        self._ensure_initialized()
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

    def record_new_profile(self, twitter_handle, notion_page_id, source_username, category=None):
        """
        Records a new profile or updates an existing one.
        Uses the two-table structure: processed_profiles and source_relationships
        """
        self._ensure_initialized()
        twitter_handle = self._normalize_handle(twitter_handle)
        source_username = self._normalize_handle(source_username)
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current timestamp
            now = datetime.now().isoformat()
            
            # Check if profile already exists
            cursor.execute(
                """
                SELECT twitter_handle, category
                FROM processed_profiles
                WHERE twitter_handle = ? COLLATE NOCASE
                ORDER BY
                    (notion_page_id IS NOT NULL) DESC,
                    first_discovered_date ASC,
                    last_updated_date DESC,
                    rowid DESC
                LIMIT 1
                """,
                (twitter_handle,),
            )
            existing = cursor.fetchone()
            
            canonical_handle = twitter_handle
            if existing:
                canonical_handle = existing[0]
                # Update existing profile
                if notion_page_id is None:
                    cursor.execute("""
                        UPDATE processed_profiles
                        SET last_updated_date = ?, category = COALESCE(?, category)
                        WHERE twitter_handle = ?
                    """, (now, category, canonical_handle))
                else:
                    cursor.execute("""
                        UPDATE processed_profiles 
                        SET last_updated_date = ?, notion_page_id = ?, category = COALESCE(?, category)
                        WHERE twitter_handle = ?
                    """, (now, notion_page_id, category, canonical_handle))
            else:
                # Insert new profile
                cursor.execute("""
                    INSERT INTO processed_profiles 
                    (twitter_handle, first_discovered_date, last_updated_date, notion_page_id, category)
                    VALUES (?, ?, ?, ?, ?)
                """, (twitter_handle, now, now, notion_page_id, category))
            
            # Record source relationship
            cursor.execute("""
                INSERT OR IGNORE INTO source_relationships 
                (twitter_handle, discovered_by_handle, discovery_date)
                VALUES (?, ?, ?)
            """, (canonical_handle, source_username, now))
            
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
        twitter_handle = self._normalize_handle(twitter_handle)
        source_username = self._normalize_handle(source_username)
        # No need for _ensure_initialized() here since _execute_query calls it
        query = """
        SELECT p.twitter_handle, p.notion_page_id, p.last_updated_date
        FROM processed_profiles p
        INNER JOIN source_relationships sr ON p.twitter_handle = sr.twitter_handle
        WHERE p.twitter_handle = ? COLLATE NOCASE AND sr.discovered_by_handle = ? COLLATE NOCASE
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
        twitter_handle = self._normalize_handle(twitter_handle)
        query = """
        SELECT *
        FROM processed_profiles
        WHERE twitter_handle = ? COLLATE NOCASE
        ORDER BY
            (notion_page_id IS NOT NULL) DESC,
            first_discovered_date ASC,
            last_updated_date DESC,
            rowid DESC
        LIMIT 1
        """
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
        twitter_handle = self._normalize_handle(twitter_handle)
        now = datetime.now().isoformat()
        query = "UPDATE processed_profiles SET last_updated_date = ? WHERE twitter_handle = ? COLLATE NOCASE"
        try:
            self._execute_query(query, (now, twitter_handle))
        except Exception as e:
            logger.error(f"Failed to update last seen for {twitter_handle}: {e}")

    def add_source_relationship(self, twitter_handle, source_username):
        """
        Add a new source relationship (or ignore if already exists)
        """
        twitter_handle = self._normalize_handle(twitter_handle)
        source_username = self._normalize_handle(source_username)
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
        twitter_handle = self._normalize_handle(twitter_handle)
        query = """
        SELECT discovered_by_handle, discovery_date 
        FROM source_relationships 
        WHERE twitter_handle = ? COLLATE NOCASE
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
