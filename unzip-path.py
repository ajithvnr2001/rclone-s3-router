"""
Unzip Multiple ZIPs - One at a time
Flow: Unzip to local -> Move to drive -> Log -> Delete local -> Next
"""

import os
import zipfile
import shutil

# Configuration
ZIP_FOLDER = "/content/drive/MyDrive/file_migration_zips"
LOCAL_TEMP = "/content/unzip_temp"
PROGRESS_LOG = "/content/drive/MyDrive/file_migration_zips/unzip_progress.log"


def load_done():
    """Load completed zips."""
    done = set()
    if os.path.exists(PROGRESS_LOG):
        with open(PROGRESS_LOG, "r") as f:
            done = set(line.strip() for line in f if line.strip())
        print(f"Resume: {len(done)} already done")
    return done


def log_done(name):
    """Log completed zip."""
    with open(PROGRESS_LOG, "a") as f:
        f.write(name + "\n")


def main():
    print("=" * 50)
    print("UNZIP MULTIPLE - ONE AT A TIME")
    print(f"Source: {ZIP_FOLDER}")
    print("=" * 50)
    
    done = load_done()
    
    # Get all zips
    zips = [f for f in os.listdir(ZIP_FOLDER) if f.endswith(".zip")]
    print(f"Found {len(zips)} zips, {len(done)} already done")
    print("-" * 50)
    
    processed = 0
    errors = 0
    
    for i, zip_name in enumerate(zips):
        # Skip if done
        if zip_name in done:
            continue
        
        print(f"[{i+1}/{len(zips)}] {zip_name}")
        
        zip_path = os.path.join(ZIP_FOLDER, zip_name)
        folder_name = zip_name[:-4]  # Remove .zip
        local_path = os.path.join(LOCAL_TEMP, folder_name)
        dest_path = os.path.join(ZIP_FOLDER, folder_name)
        
        try:
            # 1. Clean local temp
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
            os.makedirs(LOCAL_TEMP)
            
            # 2. Unzip to local
            print("  -> Unzipping to local...")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(local_path)
            
            # 3. Move to Drive
            print("  -> Moving to Drive...")
            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
            shutil.move(local_path, dest_path)
            
            # 4. Log it
            log_done(zip_name)
            print("  -> Logged!")
            
            # 5. Delete local
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
            print("  -> Local cleaned!")
            
            processed += 1
            print("  -> DONE!")
            
        except Exception as e:
            print(f"  -> ERROR: {e}")
            errors += 1
            # Clean up on error
            if os.path.exists(LOCAL_TEMP):
                shutil.rmtree(LOCAL_TEMP)
    
    print("\n" + "=" * 50)
    print("COMPLETE!")
    print(f"  Processed: {processed}")
    print(f"  Errors: {errors}")
    print(f"  Skipped: {len(done)}")


if __name__ == "__main__":
    main()
