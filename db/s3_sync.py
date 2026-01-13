import os
import sqlite3
import tempfile
import zipfile
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from utils.logger import logger
from config import (
    DB_DIR,
    S3_AI_TWEETS_ZIP_PREFIX,
    S3_BUCKET,
    S3_DB_KEY,
    S3_LOGS_PREFIX,
    S3_REGION,
    S3_UPLOAD_AI_TWEETS_ZIP,
    S3_UPLOAD_LOGS,
    USE_S3_SYNC,
)

class S3DatabaseSync:
    def __init__(self):
        """Initialize S3 client with configured region"""
        if not USE_S3_SYNC:
            logger.log("S3 sync is disabled")
            return
            
        try:
            self.s3 = boto3.client('s3', region_name=S3_REGION)
            self.bucket = S3_BUCKET
            self.key = S3_DB_KEY
            self.local_path = os.path.join(DB_DIR, "twitter_profiles.db")
            
            # Test S3 access
            self._test_s3_access()
            logger.log(f"S3 sync initialized - Bucket: {self.bucket}, Key: {self.key}")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure AWS credentials.")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 sync: {e}")
            raise
    
    def _test_s3_access(self):
        """Test if we can access the S3 bucket"""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logger.error(f"S3 bucket '{self.bucket}' does not exist")
                raise
            elif error_code == 403:
                logger.error(f"Access denied to S3 bucket '{self.bucket}'")
                raise
            else:
                logger.error(f"Error accessing S3 bucket: {e}")
                raise
    
    def _get_database_stats(self, db_path):
        """Get statistics about the database"""
        stats = {
            'total_profiles': 0,
            'total_relationships': 0,
            'file_size_mb': 0,
            'last_profile_date': None,
            'unique_sources': 0
        }
        
        try:
            # Get file size
            if os.path.exists(db_path):
                stats['file_size_mb'] = os.path.getsize(db_path) / 1024 / 1024
            
            # Connect to database for counts
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get profile count
            cursor.execute("SELECT COUNT(*) FROM processed_profiles")
            stats['total_profiles'] = cursor.fetchone()[0]
            
            # Get relationship count
            cursor.execute("SELECT COUNT(*) FROM source_relationships")
            stats['total_relationships'] = cursor.fetchone()[0]
            
            # Get unique source count
            cursor.execute("SELECT COUNT(DISTINCT discovered_by_handle) FROM source_relationships")
            stats['unique_sources'] = cursor.fetchone()[0]
            
            # Get most recent profile date
            cursor.execute("SELECT MAX(last_updated_date) FROM processed_profiles")
            last_date = cursor.fetchone()[0]
            if last_date:
                stats['last_profile_date'] = last_date
            
            conn.close()
        except Exception as e:
            logger.debug(f"Could not get database stats: {e}")
        
        return stats
    
    async def smart_download(self):
        """Download from S3 if local doesn't exist OR if S3 is newer"""
        if not USE_S3_SYNC:
            return
            
        try:
            # Check if local database exists
            local_exists = os.path.exists(self.local_path)
            
            if not local_exists:
                # No local database - MUST download from S3
                logger.log(f"⚠️  No local database found at {self.local_path}")
                logger.log(f"📥 Downloading database from S3 (required)...")
                
                try:
                    await self.download_latest()
                    
                    # Validate the download succeeded
                    if not os.path.exists(self.local_path):
                        logger.error("❌ CRITICAL: S3 download failed - no local database exists!")
                        logger.error("Cannot proceed without database. Exiting.")
                        raise RuntimeError("Failed to download database from S3 and no local database exists")
                    
                    # Validate the downloaded database has data
                    stats = self._get_database_stats(self.local_path)
                    if stats['total_profiles'] == 0:
                        logger.error("❌ CRITICAL: Downloaded database is empty!")
                        logger.error(f"Expected database with profiles, got empty database")
                        raise RuntimeError("Downloaded database from S3 is empty")
                    
                    logger.log(f"✅ Successfully downloaded database with {stats['total_profiles']:,} profiles")
                    return
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.error("❌ CRITICAL: No database found in S3 and no local database exists!")
                        logger.error("Cannot proceed without any database. Please provide a database.")
                        raise RuntimeError("No database available - neither local nor in S3")
                    else:
                        logger.error(f"❌ CRITICAL: Failed to download database from S3: {e}")
                        raise RuntimeError(f"S3 download failed and no local database exists: {e}")
            else:
                # Local database exists - compare with S3
                local_mtime = os.path.getmtime(self.local_path)
                local_mtime_str = datetime.fromtimestamp(local_mtime).strftime('%Y-%m-%d %H:%M:%S')
                local_stats = self._get_database_stats(self.local_path)
                logger.log(f"📁 Local database found: {local_stats['total_profiles']:,} profiles, last modified: {local_mtime_str}")
                
                # Get S3 object metadata
                try:
                    s3_info = self.s3.head_object(Bucket=self.bucket, Key=self.key)
                    s3_mtime = s3_info['LastModified'].timestamp()
                    s3_mtime_str = s3_info['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
                    s3_size_mb = s3_info['ContentLength'] / 1024 / 1024
                    logger.log(f"☁️  S3 database: {s3_size_mb:.2f} MB, last modified: {s3_mtime_str}")
                    
                    # Compare timestamps
                    if s3_mtime <= local_mtime:
                        logger.log(f"✅ Local database is newer or same as S3, keeping local version")
                        return
                        
                    logger.log(f"📥 S3 database is newer, downloading...")
                    await self.download_latest()
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.log("No database in S3, using local database")
                    else:
                        logger.error(f"Error checking S3 database: {e}")
                        logger.log("Continuing with local database")
                        
        except RuntimeError:
            # Re-raise critical errors
            raise
        except Exception as e:
            logger.error(f"Unexpected error in smart download: {e}")
            # If we have a local database, we can continue
            if os.path.exists(self.local_path):
                logger.warn("Continuing with existing local database")
            else:
                raise RuntimeError(f"No local database and download failed: {e}")
            
    async def download_latest(self):
        """Download the latest database from S3 to local"""
        if not USE_S3_SYNC:
            return
            
        try:
            # Check if object exists and get metadata
            try:
                s3_info = self.s3.head_object(Bucket=self.bucket, Key=self.key)
                s3_size_mb = s3_info['ContentLength'] / 1024 / 1024
                s3_last_modified = s3_info['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
                s3_metadata = s3_info.get('Metadata', {})
                
                logger.log(f"📊 Found database in S3:")
                logger.log(f"   Source: s3://{self.bucket}/{self.key}")
                logger.log(f"   Size: {s3_size_mb:.2f} MB")
                logger.log(f"   Last Modified: {s3_last_modified}")
                if s3_metadata.get('uploaded-by'):
                    logger.log(f"   Uploaded By: {s3_metadata.get('uploaded-by')}")
                if s3_metadata.get('source-machine'):
                    logger.log(f"   Source Machine: {s3_metadata.get('source-machine')}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.warn(f"Database not found in S3 ({self.key}). Will use local database if it exists.")
                    return
                raise
            
            # Show existing local database stats if it exists
            local_exists = os.path.exists(self.local_path)
            if local_exists:
                logger.log(f"\n📁 Existing local database found:")
                local_stats = self._get_database_stats(self.local_path)
                logger.log(f"   Path: {self.local_path}")
                logger.log(f"   Size: {local_stats['file_size_mb']:.2f} MB")
                logger.log(f"   Profiles: {local_stats['total_profiles']:,}")
                logger.log(f"   Relationships: {local_stats['total_relationships']:,}")
                logger.log(f"   Unique Sources: {local_stats['unique_sources']}")
                if local_stats['last_profile_date']:
                    logger.log(f"   Last Updated: {local_stats['last_profile_date']}")
            
            # Download the file
            logger.log(f"\n⬇️  Downloading database from S3...")
            absolute_path = os.path.abspath(self.local_path)
            self.s3.download_file(self.bucket, self.key, self.local_path)
            
            # Get stats of downloaded database
            stats = self._get_database_stats(self.local_path)
            
            logger.log(f"\n✅ Successfully downloaded database from S3")
            logger.log(f"   Downloaded to: {absolute_path}")
            logger.log(f"   File size: {stats['file_size_mb']:.2f} MB")
            logger.log(f"   Total profiles: {stats['total_profiles']:,}")
            logger.log(f"   Total relationships: {stats['total_relationships']:,}")
            logger.log(f"   Unique sources: {stats['unique_sources']}")
            if stats['last_profile_date']:
                logger.log(f"   Most recent profile: {stats['last_profile_date']}")
            
            # Show what changed if there was a local database
            if local_exists:
                profile_diff = stats['total_profiles'] - local_stats['total_profiles']
                rel_diff = stats['total_relationships'] - local_stats['total_relationships']
                if profile_diff != 0 or rel_diff != 0:
                    logger.log(f"\n📈 Changes from local version:")
                    if profile_diff != 0:
                        logger.log(f"   Profiles: {'+' if profile_diff > 0 else ''}{profile_diff}")
                    if rel_diff != 0:
                        logger.log(f"   Relationships: {'+' if rel_diff > 0 else ''}{rel_diff}")
            
        except ClientError as e:
            logger.error(f"Failed to download database from S3: {e}")
            # Don't raise - allow the app to continue with local DB if it exists
            if os.path.exists(self.local_path):
                logger.warn("Using existing local database")
            else:
                raise
        except Exception as e:
            logger.error(f"Unexpected error downloading from S3: {e}")
            raise
    
    async def upload_changes(self):
        """Upload the updated database to S3 after processing completes"""
        if not USE_S3_SYNC:
            return
            
        try:
            if not os.path.exists(self.local_path):
                logger.error(f"Local database not found at {self.local_path}")
                return
            
            # Get database stats before upload
            stats = self._get_database_stats(self.local_path)
            
            logger.log(f"\n📤 Preparing to upload database to S3:")
            logger.log(f"   Source: {os.path.abspath(self.local_path)}")
            logger.log(f"   Destination: s3://{self.bucket}/{self.key}")
            logger.log(f"   File size: {stats['file_size_mb']:.2f} MB")
            logger.log(f"   Total profiles: {stats['total_profiles']:,}")
            logger.log(f"   Total relationships: {stats['total_relationships']:,}")
            logger.log(f"   Unique sources: {stats['unique_sources']}")
            if stats['last_profile_date']:
                logger.log(f"   Most recent profile: {stats['last_profile_date']}")
            
            logger.log(f"\n⬆️  Uploading to S3...")
            
            # Upload with metadata
            self.s3.upload_file(
                self.local_path, 
                self.bucket, 
                self.key,
                ExtraArgs={
                    'Metadata': {
                        'uploaded-by': os.environ.get('USER', 'unknown'),
                        'source-machine': os.environ.get('HOSTNAME', os.environ.get('COMPUTERNAME', 'unknown')),
                        'upload-time': datetime.now().isoformat(),
                        'profile-count': str(stats['total_profiles']),
                        'relationship-count': str(stats['total_relationships'])
                    }
                }
            )
            
            # Verify upload by checking object exists
            response = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            version_id = response.get('VersionId', 'not-versioned')
            
            logger.log(f"\n✅ Successfully uploaded database to S3")
            logger.log(f"   Version ID: {version_id}")
            logger.log(f"   Upload time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.log(f"   Uploaded by: {os.environ.get('USER', 'unknown')}")
            logger.log(f"   From machine: {os.environ.get('HOSTNAME', os.environ.get('COMPUTERNAME', 'unknown'))}")
            
        except ClientError as e:
            logger.error(f"Failed to upload database to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}")
            raise
    
    async def sync_follower_counts(self):
        """Sync only the newest follower counts file to S3"""
        if not USE_S3_SYNC:
            return
            
        try:
            from config import FOLLOWER_COUNTS_DIR
            
            if not os.path.exists(FOLLOWER_COUNTS_DIR):
                logger.log("No follower counts directory found")
                return
                
            # Find newest local file
            files = os.listdir(FOLLOWER_COUNTS_DIR)
            if not files:
                logger.log("No follower counts files to sync")
                return
                
            # Filter out backup files and find newest
            count_files = [f for f in files if f.startswith('follower_counts_') and not f.startswith('backup_')]
            if not count_files:
                logger.log("No follower counts files found")
                return
                
            newest_local = sorted(count_files, reverse=True)[0]
            local_path = os.path.join(FOLLOWER_COUNTS_DIR, newest_local)
            
            # Upload to S3 with same filename (preserves dates)
            s3_key = f"follower_counts/{newest_local}"
            
            # Check if this file already exists in S3
            try:
                self.s3.head_object(Bucket=self.bucket, Key=s3_key)
                logger.log(f"Follower counts file {newest_local} already exists in S3")
                return
            except ClientError:
                # File doesn't exist, proceed with upload
                pass
            
            # Upload the file
            logger.log(f"📤 Uploading follower counts: {newest_local}")
            self.s3.upload_file(local_path, self.bucket, s3_key)
            logger.log(f"✅ Uploaded {newest_local} to S3")
            
        except Exception as e:
            logger.error(f"Error syncing follower counts: {e}")
            # Don't fail the whole process
    
    async def download_latest_counts(self):
        """Download the newest counts file from S3 if we don't have it"""
        if not USE_S3_SYNC:
            return
            
        try:
            from config import FOLLOWER_COUNTS_DIR
            
            # Ensure directory exists
            os.makedirs(FOLLOWER_COUNTS_DIR, exist_ok=True)
            
            # List all follower_counts files in S3
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='follower_counts/follower_counts_'
            )
            
            if not response.get('Contents'):
                logger.log("No follower counts in S3")
                return
                
            # Find newest by filename (dates in filename)
            s3_files = [obj['Key'] for obj in response['Contents']]
            newest_s3 = sorted(s3_files, reverse=True)[0]
            
            # Download only if we don't have it locally
            local_filename = os.path.basename(newest_s3)
            local_path = os.path.join(FOLLOWER_COUNTS_DIR, local_filename)
            
            if os.path.exists(local_path):
                logger.log(f"Already have latest follower counts: {local_filename}")
                return
                
            # Download the file
            logger.log(f"📥 Downloading follower counts: {local_filename}")
            self.s3.download_file(self.bucket, newest_s3, local_path)
            logger.log(f"✅ Downloaded {local_filename} from S3")
            
        except Exception as e:
            logger.error(f"Error downloading follower counts: {e}")
            # Don't fail the whole process
    
    async def list_versions(self, limit=10):
        """List recent versions of the database in S3 (useful for debugging)"""
        if not USE_S3_SYNC:
            return []
            
        try:
            response = self.s3.list_object_versions(
                Bucket=self.bucket,
                Prefix=self.key,
                MaxKeys=limit
            )
            
            versions = []
            for version in response.get('Versions', []):
                versions.append({
                    'version_id': version['VersionId'],
                    'last_modified': version['LastModified'].isoformat(),
                    'size': version['Size'],
                    'is_latest': version['IsLatest']
                })
            
            return versions
        except Exception as e:
            logger.error(f"Failed to list S3 versions: {e}")
            return []

    @staticmethod
    def _normalize_prefix(prefix_value):
        if prefix_value is None:
            return ""
        normalized = str(prefix_value).strip().lstrip("/")
        return normalized.rstrip("/")

    @staticmethod
    def _directory_has_files(directory_path):
        for _, _, files in os.walk(directory_path):
            if files:
                return True
        return False

    @staticmethod
    def _zip_directory(source_dir, zip_path):
        folder_name = os.path.basename(os.path.abspath(source_dir)) or "ai_tweets"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(source_dir):
                for filename in files:
                    full_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(full_path, start=source_dir)
                    arcname = os.path.join(folder_name, rel_path)
                    zf.write(full_path, arcname=arcname)

    async def upload_log_file(self, log_file_path):
        """Upload a local run log file (app_*.log) to S3."""
        if not USE_S3_SYNC or not S3_UPLOAD_LOGS:
            return

        if not log_file_path:
            logger.log("No log file path provided; skipping log upload.")
            return

        if not os.path.exists(log_file_path):
            logger.log(f"Log file not found: {log_file_path}. Skipping upload.")
            return

        prefix = self._normalize_prefix(S3_LOGS_PREFIX)
        filename = os.path.basename(log_file_path)
        s3_key = f"{prefix}/{filename}" if prefix else filename

        try:
            logger.log(f"📤 Uploading run log to S3: s3://{self.bucket}/{s3_key}")
            self.s3.upload_file(
                log_file_path,
                self.bucket,
                s3_key,
                ExtraArgs={
                    "ContentType": "text/plain",
                    "Metadata": {
                        "uploaded-by": os.environ.get("USER", "unknown"),
                        "source-machine": os.environ.get("HOSTNAME", os.environ.get("COMPUTERNAME", "unknown")),
                        "upload-time": datetime.now().isoformat(),
                    },
                },
            )
            logger.log(f"✅ Uploaded run log: {filename}")
        except Exception as e:
            logger.error(f"Failed to upload run log to S3: {e}")
            raise

    async def upload_ai_tweets_zip(self, ai_tweets_dir, run_id=None):
        """Zip the ai_tweets directory and upload it to S3 as a single archive."""
        if not USE_S3_SYNC or not S3_UPLOAD_AI_TWEETS_ZIP:
            return

        if not ai_tweets_dir or not os.path.isdir(ai_tweets_dir):
            logger.log("No ai_tweets directory found; skipping AI tweets archive upload.")
            return

        if not self._directory_has_files(ai_tweets_dir):
            logger.log("ai_tweets directory is empty; skipping AI tweets archive upload.")
            return

        prefix = self._normalize_prefix(S3_AI_TWEETS_ZIP_PREFIX)
        timestamp = run_id or datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        zip_filename = f"{timestamp}.zip"
        s3_key = f"{prefix}/{zip_filename}" if prefix else zip_filename

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(prefix="ai_tweets_", suffix=".zip", delete=False) as tmp:
                tmp_path = tmp.name

            self._zip_directory(ai_tweets_dir, tmp_path)

            size_mb = os.path.getsize(tmp_path) / 1024 / 1024
            logger.log(f"📦 Created AI tweets archive: {zip_filename} ({size_mb:.2f} MB)")
            logger.log(f"📤 Uploading AI tweets archive to S3: s3://{self.bucket}/{s3_key}")

            self.s3.upload_file(
                tmp_path,
                self.bucket,
                s3_key,
                ExtraArgs={
                    "ContentType": "application/zip",
                    "Metadata": {
                        "uploaded-by": os.environ.get("USER", "unknown"),
                        "source-machine": os.environ.get("HOSTNAME", os.environ.get("COMPUTERNAME", "unknown")),
                        "upload-time": datetime.now().isoformat(),
                        "run-id": str(timestamp),
                    },
                },
            )
            logger.log(f"✅ Uploaded AI tweets archive: {zip_filename}")
        except Exception as e:
            logger.error(f"Failed to upload AI tweets archive to S3: {e}")
            raise
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception as cleanup_error:
                    logger.debug(f"Could not delete temp AI tweets zip {tmp_path}: {cleanup_error}")
