"""
Unzip Multiple ZIPs - Hybrid with rclone fallback
Try move folder first, if fails -> use rclone (handles all files)
"""

import os
import zipfile
import shutil
import subprocess

# Configuration
ZIP_FOLDER = "/content/drive/MyDrive/file_migration_zips"
LOCAL_TEMP = "/content/unzip_temp"
PROGRESS_LOG = "/content/drive/MyDrive/file_migration_zips/unzip_progress.log"
RCLONE_CONFIG = "/content/rclone.conf"
GDRIVE_REMOTE = "gdrive"
GDRIVE_DEST = "file_migration_zips"  # Path in Google Drive


def load_done():
    done = set()
    if os.path.exists(PROGRESS_LOG):
        with open(PROGRESS_LOG, "r") as f:
            done = set(line.strip() for line in f if line.strip())
        print(f"Resume: {len(done)} already done")
    return done


def log_done(name):
    with open(PROGRESS_LOG, "a") as f:
        f.write(name + "\n")


def rclone_move(local_path, dest_name):
    """Use rclone to move files (handles all file types)."""
    dest = f"{GDRIVE_REMOTE}:{GDRIVE_DEST}/{dest_name}"
    cmd = [
        "rclone", "--config", RCLONE_CONFIG,
        "move", local_path, dest,
        "--progress"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stderr


def move_to_drive(local_path, dest_path, folder_name):
    """Try shutil.move first, fallback to rclone."""
    
    # Remove existing destination
    if os.path.exists(dest_path):
        shutil.rmtree(dest_path)
    
    # Try fast move first
    try:
        shutil.move(local_path, dest_path)
        return "moved (fast)", True
    except Exception as e:
        pass
    
    # Fallback: use rclone (handles all file types)
    print("  -> Using rclone (handles gdoc files)...")
    success, err = rclone_move(local_path, folder_name)
    if success:
        return "moved (rclone)", True
    else:
        return f"rclone error: {err}", False


def main():
    print("=" * 50)
    print("UNZIP MULTIPLE - HYBRID (rclone fallback)")
    print(f"Source: {ZIP_FOLDER}")
    print("=" * 50)
    
    done = load_done()
    
    zips = [f for f in os.listdir(ZIP_FOLDER) if f.endswith(".zip")]
    print(f"Found {len(zips)} zips, {len(done)} already done")
    print("-" * 50)
    
    processed = 0
    errors = 0
    
    for i, zip_name in enumerate(zips):
        if zip_name in done:
            continue
        
        print(f"[{i+1}/{len(zips)}] {zip_name}")
        
        zip_path = os.path.join(ZIP_FOLDER, zip_name)
        folder_name = zip_name[:-4]
        local_path = os.path.join(LOCAL_TEMP, folder_name)
        dest_path = os.path.join(ZIP_FOLDER, folder_name)
        
        try:
            # 1. Clean temp
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
            os.makedirs(LOCAL_TEMP)
            
            # 2. Unzip
            print("  -> Unzipping...")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(local_path)
            
            # 3. Move to Drive (hybrid)
            print("  -> Moving to Drive...")
            method, success = move_to_drive(local_path, dest_path, folder_name)
            print(f"  -> {method}")
            
            if not success:
                errors += 1
                continue
            
            # 4. Log
            log_done(zip_name)
            
            # 5. Cleanup
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
            
            processed += 1
            print("  -> DONE!")
            
        except Exception as e:
            print(f"  -> ERROR: {e}")
            errors += 1
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
    
    print("\n" + "=" * 50)
    print(f"COMPLETE! Processed: {processed}, Errors: {errors}")


if __name__ == "__main__":
    main()
