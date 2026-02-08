"""
Merge Part Folders (cgaxistextures_1, _11, _12 -> cgaxistextures)

Also merges matching subfolders inside.
With resume capabilities.
"""

import os
import re
import shutil
from collections import defaultdict

# Configuration
ROOT_FOLDER = "/content/drive/MyDrive/YOUR_FOLDER_HERE"  # UPDATE THIS
PROGRESS_LOG = os.path.join(ROOT_FOLDER, "merge_folders_progress.log")

# Pattern: folder_1, folder_11, folder_part1, etc.
PART_PATTERN = re.compile(r'^(.+?)(?:_part)?_(\d+)$', re.IGNORECASE)

# Stats
merged_count = 0
files_moved = 0
folders_deleted = 0
errors = 0


def load_done():
    done = set()
    if os.path.exists(PROGRESS_LOG):
        with open(PROGRESS_LOG, "r") as f:
            done = set(line.strip() for line in f if line.strip())
        print(f"Resume: {len(done)} already processed")
    return done


def log_done(key):
    with open(PROGRESS_LOG, "a") as f:
        f.write(key + "\n")


def find_part_groups(parent_dir):
    """Find folders matching _N pattern and group by base name."""
    try:
        items = os.listdir(parent_dir)
    except:
        return {}
    
    groups = defaultdict(list)
    for item in items:
        path = os.path.join(parent_dir, item)
        if os.path.isdir(path):
            match = PART_PATTERN.match(item)
            if match:
                base = match.group(1)
                num = int(match.group(2))
                groups[base.lower()].append((item, base, num))
    
    # Return groups with at least one part folder
    return {k: v for k, v in groups.items() if len(v) > 0}


def merge_into(src_dir, dst_dir):
    """Recursively merge src into dst, combining matching subfolders."""
    global files_moved, folders_deleted
    
    os.makedirs(dst_dir, exist_ok=True)
    
    for item in os.listdir(src_dir):
        src_path = os.path.join(src_dir, item)
        dst_path = os.path.join(dst_dir, item)
        
        if os.path.isdir(src_path):
            # Recursively merge subfolders
            merge_into(src_path, dst_path)
            # Delete empty source folder
            try:
                if not os.listdir(src_path):
                    os.rmdir(src_path)
                    folders_deleted += 1
            except:
                pass
        else:
            # Move file, handle conflicts
            if os.path.exists(dst_path):
                base, ext = os.path.splitext(item)
                counter = 1
                while os.path.exists(dst_path):
                    dst_path = os.path.join(dst_dir, f"{base}_{counter}{ext}")
                    counter += 1
            shutil.move(src_path, dst_path)
            files_moved += 1


def process_directory(dir_path, done):
    """Find and merge part folders in a directory."""
    global merged_count, folders_deleted, errors
    
    groups = find_part_groups(dir_path)
    
    for base_key, parts in groups.items():
        # Sort by part number
        parts.sort(key=lambda x: x[2])
        
        # Use first part's original base name (preserves case)
        target_name = parts[0][1]
        target_path = os.path.join(dir_path, target_name)
        
        for folder_name, base, num in parts:
            source_path = os.path.join(dir_path, folder_name)
            merge_key = f"{source_path}"
            
            if merge_key in done:
                continue
            
            if not os.path.exists(source_path):
                continue
            
            print(f"[MERGE] {folder_name} -> {target_name}")
            
            try:
                # Merge contents
                merge_into(source_path, target_path)
                
                # Delete empty source
                try:
                    if os.path.exists(source_path) and not os.listdir(source_path):
                        os.rmdir(source_path)
                        folders_deleted += 1
                        print(f"  -> Deleted empty: {folder_name}")
                except:
                    pass
                
                log_done(merge_key)
                merged_count += 1
                
            except Exception as e:
                print(f"  ERROR: {e}")
                errors += 1
    
    # Process subdirectories (after merging current level)
    try:
        for item in os.listdir(dir_path):
            path = os.path.join(dir_path, item)
            if os.path.isdir(path):
                process_directory(path, done)
    except:
        pass


def main():
    print("=" * 50)
    print("MERGE PART FOLDERS")
    print("Pattern: folder_1 + folder_11 -> folder")
    print(f"Root: {ROOT_FOLDER}")
    print("=" * 50)
    
    if not os.path.exists(ROOT_FOLDER):
        print("ERROR: Root folder not found!")
        return
    
    done = load_done()
    process_directory(ROOT_FOLDER, done)
    
    print("\n" + "=" * 50)
    print("DONE!")
    print(f"  Merged: {merged_count}")
    print(f"  Files moved: {files_moved}")
    print(f"  Folders deleted: {folders_deleted}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
