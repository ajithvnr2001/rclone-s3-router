"""
Add Folder Prefix/Suffix (fol...name...fol) - RCLONE VERSION

Recursively renames ALL folders:
  1. Sanitizes names: removes special chars & spaces, replaces with "."
  2. Wraps with "fol..." prefix and "...fol" suffix
Processes bottom-up (deepest folders first) to avoid path breakage.
Logs progress to S3 for crash-resume on Google Colab.

Example:
  My Folder (2023)/Sub Dir!  →  fol...My.Folder.2023...fol/fol...Sub.Dir...fol

Usage: !python add_folder_prefix_suffix_rclone.py
"""

import subprocess
import json
import os
import re
import time

# ============ CONFIGURATION ============
RCLONE_CONFIG = "/content/rclone.conf"
REMOTE = "gdrive"
ROOT_PATH = "Data_Migration"

# S3 for log backup (resume on crash)
S3_REMOTE = "wasabi"
S3_LOG_PATH = "data-migration-logs/folder_rename_add_progress.log"

# Local temp file
LOCAL_LOG = "/tmp/folder_rename_add_progress.log"

# Prefix and suffix
PREFIX = "fol..."
SUFFIX = "...fol"

# How often to print status (seconds)
STATUS_INTERVAL = 60

# How many renames before syncing log to S3
LOG_SYNC_INTERVAL = 50
# =======================================

# Stats
stats = {"renamed": 0, "skipped": 0, "errors": 0, "total": 0}
pending_logs = []
start_time = time.time()
last_status_time = time.time()
last_sync_time = time.time()


def run_rclone(args, timeout=300):
    """Run an rclone command and return (success, stdout, stderr)."""
    cmd = ["rclone", "--config", RCLONE_CONFIG] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "timeout"
    except Exception as e:
        return False, "", str(e)


def download_log():
    """Download progress log from S3."""
    run_rclone(["copy", f"{S3_REMOTE}:{S3_LOG_PATH}", os.path.dirname(LOCAL_LOG)])


def upload_log():
    """Flush pending logs to disk and upload to S3."""
    global last_sync_time
    if pending_logs:
        with open(LOCAL_LOG, "a") as f:
            for entry in pending_logs:
                f.write(entry + "\n")
        pending_logs.clear()
    run_rclone(["copy", LOCAL_LOG, f"{S3_REMOTE}:{os.path.dirname(S3_LOG_PATH)}/"])
    last_sync_time = time.time()


def log_done(old_path, new_path):
    """Log a completed rename."""
    pending_logs.append(f"RENAMED|{old_path}|{new_path}")
    if len(pending_logs) >= LOG_SYNC_INTERVAL:
        upload_log()
        print(f"  📤 Log synced to S3 ({stats['renamed']} renamed so far)")


def load_done_set():
    """Load already-processed paths from log for resume."""
    done = set()
    download_log()
    try:
        with open(LOCAL_LOG, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("RENAMED|"):
                    parts = line.split("|", 2)
                    if len(parts) >= 2:
                        done.add(parts[1])  # old path
        print(f"✅ Resume: {len(done)} already renamed from previous run")
    except FileNotFoundError:
        open(LOCAL_LOG, "w").close()
        print("📝 Fresh start - no previous log found")
    return done


def is_already_renamed(name):
    """Check if folder name already has the prefix and suffix."""
    return name.startswith(PREFIX) and name.endswith(SUFFIX)


def sanitize_name(name):
    """Clean folder name: replace spaces & special chars with dots.

    Rules:
      - Keep only alphanumeric, dots, hyphens, underscores
      - Replace everything else (spaces, brackets, etc.) with "."
      - Collapse consecutive dots into one
      - Strip leading/trailing dots
    """
    # Replace any non-alphanumeric/dot/hyphen/underscore with a dot
    cleaned = re.sub(r'[^\w.\-]', '.', name)
    # Collapse multiple consecutive dots
    cleaned = re.sub(r'\.{2,}', '.', cleaned)
    # Strip leading/trailing dots
    cleaned = cleaned.strip('.')
    # If everything was stripped, fall back to original (safety net)
    return cleaned if cleaned else name


def list_all_folders():
    """List all folders recursively with a single API call."""
    print("📂 Listing all folders recursively (single API call)...")
    ok, out, err = run_rclone([
        "lsjson", "--recursive", "--dirs-only",
        "--no-modtime", "--no-mimetype",
        f"{REMOTE}:{ROOT_PATH}"
    ], timeout=1800)  # 30 min timeout for huge drives

    if not ok:
        print(f"❌ Failed to list folders: {err}")
        return []

    try:
        items = json.loads(out)
        print(f"📊 Found {len(items)} folders total")
        return items
    except json.JSONDecodeError:
        print("❌ Failed to parse folder listing")
        return []


def moveto(src, dst):
    """Rename a folder using rclone moveto (server-side rename on GDrive)."""
    ok, _, err = run_rclone(["moveto", f"{REMOTE}:{src}", f"{REMOTE}:{dst}"])
    return ok, err


def print_status():
    """Print periodic status update."""
    global last_status_time
    now = time.time()
    if now - last_status_time >= STATUS_INTERVAL:
        elapsed = int(now - start_time)
        rate = stats["renamed"] / max(elapsed, 1) * 3600
        pct = (stats["renamed"] + stats["skipped"]) / max(stats["total"], 1) * 100
        print(
            f"\n📊 [{elapsed//60}m {elapsed%60}s] "
            f"Progress: {pct:.1f}% | "
            f"Renamed: {stats['renamed']} | "
            f"Skipped: {stats['skipped']} | "
            f"Errors: {stats['errors']} | "
            f"Rate: {rate:.0f}/hr\n"
        )
        last_status_time = now


def main():
    print("=" * 60)
    print("🏷️  ADD FOLDER PREFIX/SUFFIX")
    print(f"📁 Remote: {REMOTE}:{ROOT_PATH}")
    print(f"🏷️  Pattern: {PREFIX}<name>{SUFFIX}")
    print("=" * 60)

    # Load resume state
    done = load_done_set()

    # List all folders in one shot
    folders = list_all_folders()
    if not folders:
        print("❌ No folders found or listing failed!")
        return

    # Sort by depth — DEEPEST FIRST (bottom-up)
    # This ensures child folders are renamed before parents,
    # so parent paths remain valid at time of rename.
    folders.sort(key=lambda x: x["Path"].count("/"), reverse=True)

    stats["total"] = len(folders)
    print(f"\n🚀 Processing {stats['total']} folders (deepest first)...\n")

    for folder in folders:
        rel_path = folder["Path"]           # relative to ROOT_PATH
        full_path = f"{ROOT_PATH}/{rel_path}"
        name = os.path.basename(rel_path)

        print_status()

        # Skip if already renamed (name has the fol...X...fol pattern)
        if is_already_renamed(name):
            stats["skipped"] += 1
            continue

        # Skip if processed in a previous run (from log)
        if full_path in done:
            stats["skipped"] += 1
            continue

        # Sanitize name (remove special chars/spaces) then wrap
        clean = sanitize_name(name)
        new_name = f"{PREFIX}{clean}{SUFFIX}"
        parent = os.path.dirname(full_path)
        new_path = f"{parent}/{new_name}" if parent else new_name

        # Rename via rclone moveto (server-side on GDrive)
        ok, err = moveto(full_path, new_path)

        if ok:
            stats["renamed"] += 1
            log_done(full_path, new_path)
            if stats["renamed"] <= 5 or stats["renamed"] % 100 == 0:
                print(f"  ✅ [{stats['renamed']}] {name} → {new_name}")
        else:
            stats["errors"] += 1
            print(f"  ❌ FAIL: {name} → {new_name} | {err}")

    # Final sync
    upload_log()

    elapsed = int(time.time() - start_time)
    print("\n" + "=" * 60)
    print("🎉 COMPLETE!")
    print(f"  ✅ Renamed:  {stats['renamed']}")
    print(f"  ⏭️  Skipped:  {stats['skipped']}")
    print(f"  ❌ Errors:   {stats['errors']}")
    print(f"  ⏱️  Time:     {elapsed//60}m {elapsed%60}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
