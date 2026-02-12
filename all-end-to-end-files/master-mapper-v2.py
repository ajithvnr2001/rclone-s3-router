#!/usr/bin/env python3
"""
PYTHON MASTER MAPPER v2 - Production Ready
Auto-discovers all subfolders, scans each for files with sizes,
separates normal files (≤ threshold) and large files (> threshold),
and uploads both lists + folder index to Wasabi S3.

V2 FIXES (Aligned with python_zipper-v8.py and python_unzipper-v8.py):
- Bug #1: Use environment variables for credentials (security)
- Bug #2: Add S3_CONFIG with timeouts and connection pooling
- Bug #3: Fix S3 key naming to match zipper/unzipper (sanitize_name function)
- Bug #4: Replace bare except with proper exception handling
- Bug #5: Add Unicode filename handling (safe_encode_filename)
- Bug #6: Add structured logging module
- Bug #7: Add type annotations
- Bug #8: Make paths configurable via environment variables
- Bug #9: Add boto3 import check at startup
- Bug #10: Add large file threshold as environment variable
"""

import subprocess
import sys
import os
import json
import logging
from urllib.parse import quote
from typing import List, Dict, Any, Tuple, Optional

# Check boto3 early
try:
    import boto3
    import botocore.exceptions
    from botocore.config import Config
except ImportError:
    print("❌ boto3 not installed! Run: pip install boto3")
    sys.exit(1)

# ============ CONFIGURATION ============
SOURCE = os.environ.get("SOURCE", "onedrive:Work Files")
S3_BUCKET = os.environ.get("S3_BUCKET", "workfiles123")
S3_PREFIX = os.environ.get("S3_PREFIX", "work_files_zips/")

# Get credentials from environment variables (SECURE - never hardcode!)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-northeast-1.wasabisys.com")

# Configurable thresholds and paths
LARGE_FILE_THRESHOLD_GB = int(os.environ.get("LARGE_FILE_THRESHOLD_GB", "20"))
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")

# Unicode handling
UTF8_ENCODING = 'utf-8'

# ============ LOGGING SETUP ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============ S3 CONFIG WITH TIMEOUTS AND CONNECTION POOLING ============
S3_CONFIG = Config(
    connect_timeout=30,
    read_timeout=300,  # 5 minutes for large uploads
    retries={'max_attempts': 3},
    max_pool_connections=50  # Connection pooling for better performance
)

# =======================================
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"
LARGE_FILE_THRESHOLD_BYTES = LARGE_FILE_THRESHOLD_GB * 1024 * 1024 * 1024


# ============ UNICODE HANDLING (Aligned with V8) ============

def safe_encode_filename(filename: str) -> str:
    """Safely encode filenames to handle Unicode characters."""
    try:
        filename.encode('ascii')
        return filename
    except UnicodeEncodeError:
        import unicodedata
        normalized = unicodedata.normalize('NFC', filename)
        return normalized


def sanitize_name(name: str) -> str:
    """Sanitize name for S3 key while preserving Unicode.
    
    This MUST match the function in python_zipper-v8.py and python_unzipper-v8.py
    to ensure consistent S3 key naming across all scripts.
    """
    safe_name = safe_encode_filename(name)
    return quote(safe_name, safe='').replace('%20', '_').replace('%2F', '_')


# ============ S3 CLIENT ============

def get_s3_client() -> Any:
    """Create S3 client with validation and timeouts."""
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise ValueError(
            "AWS credentials not configured!\n"
            "Set environment variables:\n"
            "  export AWS_ACCESS_KEY_ID='your_access_key'\n"
            "  export AWS_SECRET_ACCESS_KEY='your_secret_key'"
        )
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        config=S3_CONFIG
    )


# ============ HELPER FUNCTIONS ============

def discover_folders() -> List[str]:
    """Auto-discover all top-level subfolders in the source path."""
    logger.info(f"Discovering folders in: {SOURCE}")
    
    cmd = [
        'rclone', 'lsf', SOURCE,
        '--dirs-only'
    ]
    
    if os.path.exists(RCLONE_CONFIG):
        cmd.extend(['--config', RCLONE_CONFIG])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
        folders = [line.strip().rstrip('/') for line in result.stdout.splitlines() if line.strip()]
        logger.info(f"Found {len(folders)} folders")
        for f in folders:
            logger.info(f"  • {f}")
        return folders
    except subprocess.CalledProcessError as e:
        logger.error(f"Error discovering folders: {e.stderr}")
        return []
    except subprocess.TimeoutExpired:
        logger.error("Timeout while discovering folders")
        return []


def save_folder_index(folders: List[str]) -> bool:
    """Save the master folder list to S3."""
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=FOLDER_INDEX_KEY,
            Body="\n".join(folders).encode(UTF8_ENCODING),
            ContentType='text/plain; charset=utf-8'
        )
        logger.info(f"Saved folder index to S3: {FOLDER_INDEX_KEY}")
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to save folder index: {e}")
        return False


def check_list_exists(s3: Any, map_key: str) -> bool:
    """Check if a file list already exists on S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=map_key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return False
        logger.warning(f"Error checking if list exists: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error checking list existence: {e}")
        return False


def scan_folder_with_sizes(folder: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Scan a folder using rclone lsjson to get file paths AND sizes.
    
    Returns:
        normal_files: list of relative paths (strings) for files ≤ threshold
        large_files: list of dicts {path, size, size_gb} for files > threshold
    """
    folder_path = f"{SOURCE}/{folder}"

    cmd = [
        'rclone', 'lsjson', folder_path,
        '-R', '--files-only', '--no-mimetype', '--no-modtime'
    ]
    
    if os.path.exists(RCLONE_CONFIG):
        cmd.extend(['--config', RCLONE_CONFIG])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=600)
        entries = json.loads(result.stdout)

        normal_files: List[str] = []
        large_files: List[Dict[str, Any]] = []

        for entry in entries:
            if not isinstance(entry, dict):
                continue
                
            path = entry.get('Path', '')
            size = entry.get('Size', 0)

            if size > LARGE_FILE_THRESHOLD_BYTES:
                large_files.append({
                    'path': path,
                    'size': size,
                    'size_gb': round(size / (1024 * 1024 * 1024), 2)
                })
            else:
                normal_files.append(path)

        return normal_files, large_files

    except subprocess.CalledProcessError as e:
        logger.error(f"Error scanning folder {folder}: {e.stderr[:80]}")
        return [], []
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout scanning folder {folder}")
        return [], []
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON from rclone for {folder}: {e}")
        return [], []


def upload_file_list(s3: Any, folder: str, normal_files: List[str]) -> bool:
    """Upload normal files list to S3."""
    # V2 FIX: Use sanitize_name to match zipper/unzipper S3 key format
    safe_name = sanitize_name(folder)
    map_key = f"{S3_PREFIX}{safe_name}_List.txt"
    
    try:
        if normal_files:
            file_list_data = "\n".join(normal_files)
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=file_list_data.encode(UTF8_ENCODING),
                ContentType='text/plain; charset=utf-8'
            )
        else:
            # Upload empty list so resume doesn't re-scan
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=b"",
                ContentType='text/plain; charset=utf-8'
            )
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to upload file list for {folder}: {e}")
        return False


def upload_large_files_list(s3: Any, folder: str, large_files: List[Dict[str, Any]]) -> bool:
    """Upload large files list to S3."""
    if not large_files:
        return True
        
    # V2 FIX: Use sanitize_name to match zipper/unzipper S3 key format
    safe_name = sanitize_name(folder)
    large_key = f"{S3_PREFIX}{safe_name}_LargeFiles.json"
    
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=large_key,
            Body=json.dumps(large_files, indent=2, ensure_ascii=False).encode(UTF8_ENCODING),
            ContentType='application/json; charset=utf-8'
        )
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to upload large files list for {folder}: {e}")
        return False


def run_mapper(force_rescan: bool = False) -> None:
    """Main mapper function."""
    logger.info("=" * 60)
    logger.info("PYTHON MASTER MAPPER v2 (Production Ready)")
    logger.info("=" * 60)
    logger.info(f"Source: {SOURCE}")
    logger.info(f"S3 Bucket: {S3_BUCKET}")
    logger.info(f"S3 Prefix: {S3_PREFIX}")
    logger.info(f"Large file threshold: {LARGE_FILE_THRESHOLD_GB} GB")
    logger.info(f"Rclone config: {RCLONE_CONFIG}")
    
    if force_rescan:
        logger.warning("FORCE RESCAN enabled - will re-scan all folders")

    # Validate credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        logger.error("AWS credentials not configured!")
        logger.info("Set environment variables:")
        logger.info("  export AWS_ACCESS_KEY_ID='your_access_key'")
        logger.info("  export AWS_SECRET_ACCESS_KEY='your_secret_key'")
        return

    # Get S3 client
    try:
        s3 = get_s3_client()
        # Test connection
        s3.head_bucket(Bucket=S3_BUCKET)
        logger.info("S3 connection successful")
    except Exception as e:
        logger.error(f"S3 connection failed: {e}")
        return

    # 1. Auto-discover folders
    folders = discover_folders()
    if not folders:
        logger.error("No folders found. Exiting.")
        return

    # 2. Save folder index to S3
    if not save_folder_index(folders):
        logger.error("Failed to save folder index. Exiting.")
        return

    # 3. Resume check
    if not force_rescan:
        already_done: List[str] = []
        remaining: List[str] = []
        
        for folder in folders:
            # V2 FIX: Use sanitize_name for consistent key naming
            safe_name = sanitize_name(folder)
            map_key = f"{S3_PREFIX}{safe_name}_List.txt"
            
            if check_list_exists(s3, map_key):
                already_done.append(folder)
            else:
                remaining.append(folder)

        if already_done:
            logger.info(f"Resume: {len(already_done)} folders already mapped, skipping")
            for f in already_done:
                logger.info(f"  ⏭️  {f}")

        if not remaining:
            logger.info("All folders already mapped! Use force_rescan=True to re-scan.")
            return

        folders = remaining

    # 4. Scan each folder with size detection
    logger.info(f"Scanning {len(folders)} folders (with size detection)...")

    total_normal = 0
    total_large = 0
    failed_folders: List[str] = []

    for folder in folders:
        logger.info(f"Scanning: {folder}")

        normal_files, large_files = scan_folder_with_sizes(folder)

        # Upload normal files list
        if not upload_file_list(s3, folder, normal_files):
            failed_folders.append(folder)
            continue

        # Upload large files list
        if large_files:
            if not upload_large_files_list(s3, folder, large_files):
                logger.warning(f"Failed to upload large files list for {folder}")

        total_normal += len(normal_files)
        total_large += len(large_files)

        large_info = f", {len(large_files)} large" if large_files else ""
        logger.info(f"  ✅ {folder}: {len(normal_files)} files{large_info}")

        # Print large file details
        if large_files:
            for lf in large_files:
                logger.info(f"    🔴 LARGE: {lf['path']} ({lf['size_gb']} GB)")

    # Summary
    logger.info("=" * 60)
    logger.info("MAPPING COMPLETE!")
    logger.info(f"Normal files: {total_normal}")
    logger.info(f"Large files: {total_large} (will be transferred directly)")
    
    if failed_folders:
        logger.warning(f"Failed folders: {len(failed_folders)}")
        for f in failed_folders:
            logger.warning(f"  ❌ {f}")


if __name__ == "__main__":
    run_mapper()
