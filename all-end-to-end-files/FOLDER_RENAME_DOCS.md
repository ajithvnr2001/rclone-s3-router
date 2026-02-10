# Folder Rename Scripts — Full Documentation

Two rclone-based Python scripts for bulk-renaming folders on Google Drive, designed for Google Colab with crash-resume via S3.

---

## Scripts Overview

| Script | Purpose |
|---|---|
| `add_folder_prefix_suffix_rclone.py` | Sanitizes names + wraps with `fol...name...fol` |
| `undo_folder_prefix_suffix_rclone.py` | Removes `fol.../...fol` wrapper, keeps clean name |

---

## 1. `add_folder_prefix_suffix_rclone.py`

### What It Does

Recursively renames **every folder** under `ROOT_PATH` in two steps:

1. **Sanitize** — removes spaces, special characters, brackets, etc. and replaces them with `.`
2. **Wrap** — adds `fol...` prefix and `...fol` suffix

### Sanitization Rules

| Rule | Example |
|---|---|
| Spaces → `.` | `My Folder` → `My.Folder` |
| Special chars → `.` | `Files (2023)` → `Files.2023` |
| Consecutive dots collapsed | `a..b...c` → `a.b.c` |
| Leading/trailing dots stripped | `.hello.` → `hello` |
| Allowed chars kept as-is | `a-z A-Z 0-9 . - _` |

### Name Transformation Examples

| Original Folder Name | Sanitized | Final Renamed |
|---|---|---|
| `My Documents` | `My.Documents` | `fol...My.Documents...fol` |
| `Project (Backup) [v2]` | `Project.Backup.v2` | `fol...Project.Backup.v2...fol` |
| `hello_world-2023` | `hello_world-2023` | `fol...hello_world-2023...fol` |
| `Sub Dir!@#` | `Sub.Dir` | `fol...Sub.Dir...fol` |

### Full Tree Example

```
BEFORE:                                    AFTER:
Data_Migration/                            Data_Migration/
├── My Projects/                           ├── fol...My.Projects...fol/
│   ├── Client (2023)/                     │   ├── fol...Client.2023...fol/
│   │   └── Final Report/                  │   │   └── fol...Final.Report...fol/
│   └── Internal Docs/                     │   └── fol...Internal.Docs...fol/
└── Backup [old]/                          └── fol...Backup.old...fol/
    └── Archive Files/                         └── fol...Archive.Files...fol/
```

### Processing Order — Bottom-Up

Folders are sorted **deepest first** before renaming. This is critical:

```
Step 1: Data_Migration/My Projects/Client (2023)/Final Report
        → Data_Migration/My Projects/Client (2023)/fol...Final.Report...fol

Step 2: Data_Migration/My Projects/Client (2023)
        → Data_Migration/My Projects/fol...Client.2023...fol

Step 3: Data_Migration/My Projects
        → Data_Migration/fol...My.Projects...fol
```

> **Why bottom-up?** If we renamed a parent first, all child paths would break. By renaming children first, parent paths remain valid at the time of each rename.

### How It Works Internally

```
1. rclone lsjson --recursive --dirs-only  →  Get ALL folders (single API call)
2. Sort by path depth (deepest first)
3. For each folder:
   a. Check if name already has fol.../...fol → skip
   b. Check if in done-set from previous run → skip
   c. Sanitize name (remove special chars)
   d. rclone moveto old_path new_path  →  Server-side rename on GDrive
   e. Log to progress file
4. Sync log to S3 every 50 renames
```

---

## 2. `undo_folder_prefix_suffix_rclone.py`

### What It Does

Recursively **removes** the `fol...` prefix and `...fol` suffix from all folder names.

> **Important:** This restores to the **sanitized** (clean) name, not the original name with spaces/special chars. The original names are not stored or recoverable.

### Undo Examples

| Current Name | After Undo |
|---|---|
| `fol...My.Documents...fol` | `My.Documents` |
| `fol...Project.Backup.v2...fol` | `Project.Backup.v2` |
| `fol...hello_world-2023...fol` | `hello_world-2023` |

### Processing Order

Same bottom-up approach as the add script — deepest folders first.

### How It Works Internally

```
1. rclone lsjson --recursive --dirs-only  →  Get ALL folders
2. Sort by path depth (deepest first)
3. For each folder:
   a. Check if name does NOT have fol.../...fol → skip (already undone)
   b. Check if in done-set from previous run → skip
   c. Strip prefix/suffix to get clean name
   d. rclone moveto old_path new_path
   e. Log to progress file
4. Sync log to S3 every 50 renames
```

---

## Resume on Crash

Both scripts handle Colab crashes/disconnects gracefully with **two layers of protection**:

### Layer 1: Pattern Detection (Primary)

On restart, the script re-lists all folders and checks each name:

| Script | Skip condition |
|---|---|
| **Add** | Name already starts with `fol...` and ends with `...fol` |
| **Undo** | Name does NOT have the `fol.../...fol` pattern |

This is fully **idempotent** — safe to run any number of times without double-wrapping.

### Layer 2: S3 Progress Log (Backup)

A progress log is synced to Wasabi S3 every 50 renames:

```
RENAMED|Data_Migration/My Projects|Data_Migration/fol...My.Projects...fol
RENAMED|Data_Migration/Backup [old]|Data_Migration/fol...Backup.old...fol
```

On restart, paths in the log are also skipped (belt-and-suspenders).

---

## Configuration

At the top of each script:

```python
# ============ CONFIGURATION ============
RCLONE_CONFIG = "/content/rclone.conf"   # Path to rclone config file
REMOTE = "gdrive"                        # GDrive remote name in rclone.conf
ROOT_PATH = "Data_Migration"             # Root folder to process (NOT renamed itself)

# S3 for log backup (resume on crash)
S3_REMOTE = "wasabi"                     # S3-compatible remote in rclone.conf
S3_LOG_PATH = "data-migration-logs/folder_rename_add_progress.log"

# Tuning
STATUS_INTERVAL = 60                     # Print status every N seconds
LOG_SYNC_INTERVAL = 50                   # Sync log to S3 every N renames
```

---

## Usage on Google Colab

### Prerequisites

1. `rclone.conf` at `/content/rclone.conf` with `gdrive` and `wasabi` remotes configured
2. rclone installed (`!curl https://rclone.org/install.sh | bash`)

### Run

```python
# Step 1: Add prefix/suffix to all folders
!python add_folder_prefix_suffix_rclone.py

# Step 2 (when needed): Undo all renames
!python undo_folder_prefix_suffix_rclone.py
```

### Output

```
============================================================
🏷️  ADD FOLDER PREFIX/SUFFIX
📁 Remote: gdrive:Data_Migration
🏷️  Pattern: fol...<name>...fol
============================================================
✅ Resume: 0 already renamed from previous run
📂 Listing all folders recursively (single API call)...
📊 Found 15432 folders total

🚀 Processing 15432 folders (deepest first)...

  ✅ [1] Final Report → fol...Final.Report...fol
  ✅ [2] Client (2023) → fol...Client.2023...fol
  ...
  ✅ [100] Archive → fol...Archive...fol

📊 [1m 30s] Progress: 12.5% | Renamed: 1920 | Skipped: 0 | Errors: 2 | Rate: 4800/hr

============================================================
🎉 COMPLETE!
  ✅ Renamed:  15430
  ⏭️  Skipped:  0
  ❌ Errors:   2
  ⏱️  Time:     3m 12s
============================================================
```

---

## Performance

| Metric | Details |
|---|---|
| **Listing** | Single `lsjson --recursive` call (efficient, paginated internally by rclone) |
| **Rename** | `rclone moveto` = server-side rename on GDrive → O(1) per folder regardless of contents |
| **Rate** | ~3000-5000 renames/hour depending on API rate limits |
| **Disk usage** | Zero — all operations are server-side, nothing downloaded |

---

## Key Design Decisions

1. **Bottom-up processing** — prevents path breakage when renaming parent folders
2. **Pattern-based idempotency** — no complex state tracking needed for resume
3. **Single recursive listing** — one API call vs thousands of per-folder calls
4. **`rclone moveto`** — server-side rename, no data transfer
5. **Sanitization on add only** — undo just strips wrapper, doesn't try to reverse sanitization
6. **`ROOT_PATH` never renamed** — only children are processed
