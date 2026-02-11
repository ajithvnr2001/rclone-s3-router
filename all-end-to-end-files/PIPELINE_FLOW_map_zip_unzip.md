# Cloud Migration Pipeline — Detailed Flow

> **Scripts:** `mapper.py` → `python_zipper.py` → `python_unzipper.py`
> **Works with:** Any rclone-supported cloud (OneDrive, GDrive, Dropbox, S3, Mega, SFTP, etc.)
> **Environment:** Google Colab (~107GB disk)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        S3 / WASABI (Hub)                            │
│                                                                      │
│  _index/folder_list.txt          ← mapper creates                    │
│  FolderA_List.txt                ← mapper creates (normal files)     │
│  FolderA_LargeFiles.json         ← mapper creates (files > 20GB)    │
│  FolderA_Part1.zip               ← zipper creates                   │
│  FolderA_Part2.zip               ← zipper creates                   │
│  FolderA_Part2_Split1.zip        ← zipper creates (disk/size split) │
│  _progress/zipper_progress.json  ← zipper updates                   │
│  _progress/unzipper_progress.json← unzipper updates                  │
└──────────────────────────────────────────────────────────────────────┘
         ▲                    ▲                    │
         │                    │                    ▼
    ┌────┴─────┐      ┌──────┴──────┐     ┌──────────────┐
    │ mapper.py│      │  zipper.py  │     │ unzipper.py  │
    │ (scan)   │      │ (zip+upload)│     │ (download+   │
    └────┬─────┘      └──────┬──────┘     │  unzip+upload│
         │                   │             └──────┬───────┘
         ▼                   │                    │
  ┌──────────────┐          │                    ▼
  │   SOURCE     │──────────┘             ┌──────────────┐
  │ (any cloud)  │ ← reads files          │ DESTINATION  │
  └──────────────┘                        │ (any cloud)  │
                                          └──────────────┘
```

---

## 1. MAPPER.PY — Scan & Index

### Purpose
Scan the SOURCE remote once, build file lists, and store them on S3 for the zipper and unzipper to consume.

### Step-by-Step Flow

```
START
  │
  ▼
┌─────────────────────────────────┐
│ 1. Connect to S3 (boto3)       │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 2. Discover folders             │
│    rclone lsf SOURCE            │
│    --dirs-only                  │
│    → Gets: ["Folder A",        │
│             "Folder B", ...]    │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 3. Save folder index to S3     │
│    → _index/folder_list.txt    │
│    (one folder name per line)   │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 4. Resume check                │
│    For each folder:             │
│    Does FolderName_List.txt     │
│    already exist on S3?         │
│    YES → skip (already mapped)  │
│    NO  → add to scan queue      │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────────────────────────────┐
│ 5. For each remaining folder:                           │
│                                                         │
│    rclone lsjson SOURCE/FolderName                      │
│    -R --files-only --no-mimetype --no-modtime           │
│    → Returns JSON: [{Path, Size}, ...]                  │
│                                                         │
│    For each file:                                       │
│    ├── Size ≤ 20GB → add to normal_files list           │
│    └── Size > 20GB → add to large_files list            │
│                                                         │
│    Upload to S3:                                        │
│    ├── FolderName_List.txt     (normal file paths)      │
│    └── FolderName_LargeFiles.json  (path + size)        │
│                                                         │
│    Even if 0 normal files → upload empty _List.txt      │
│    (so resume check doesn't re-scan this folder)        │
└─────────────────────────────────────────────────────────┘
           ▼
         DONE
```

### What's stored on S3 after mapper runs

```
work_files_zips/
├── _index/
│   └── folder_list.txt              "Folder A\nFolder B\nFolder C"
├── Folder_A_List.txt                "Reports/Q1.pdf\nContracts/deal.docx\n..."
├── Folder_A_LargeFiles.json         [{"path":"BigVideo.mp4","size":32212254720,"size_gb":30.0}]
├── Folder_B_List.txt                "docs/readme.txt\nimages/photo.jpg\n..."
└── Folder_C_List.txt                ""  (empty — no normal files)
```

---

## 2. PYTHON_ZIPPER.PY — Download, Zip, Upload + Large File Transfer

### Purpose
Read file lists from S3, download files from SOURCE, zip them in batches ≤20GB, upload zips to S3. Large files bypass S3 and go directly to DESTINATION.

### Step-by-Step Flow

```
START
  │
  ▼
┌─────────────────────────────────┐
│ 1. Install dependencies        │
│    apt-get install zip          │
│    Install rclone if missing    │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 2. Load progress from S3       │
│    zipper_progress.json         │
│    Shows: which folders done,   │
│    which parts done, etc.       │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 3. Fetch folder list from S3   │
│    _index/folder_list.txt       │
│    → ["Folder A", "Folder B"]  │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────────────────────────────┐
│ 4. FOR EACH FOLDER:                                     │
│                                                         │
│    Is folder_complete in progress?                      │
│    YES → print "⏭️ Skipping" → NEXT FOLDER             │
│    NO  → continue                                       │
│                                                         │
│    Fetch normal files: FolderName_List.txt from S3      │
│    Fetch large files: FolderName_LargeFiles.json        │
│    Filter out already-completed files (from progress)   │
│                                                         │
│    ┌────────────── IN PARALLEL ──────────────┐          │
│    │                                          │          │
│    │  THREAD 1: Large File Transfer           │          │
│    │  (if large files exist)                  │          │
│    │                                          │          │
│    │  THREAD 2: Normal Zip Pipeline           │          │
│    │  (ProcessPoolExecutor inside)            │          │
│    │                                          │          │
│    └──────────────────────────────────────────┘          │
│                                                         │
│    Wait for BOTH to finish (f.result())                 │
│    Mark folder complete → save progress                 │
│    → NEXT FOLDER                                        │
└─────────────────────────────────────────────────────────┘
```

### Large File Transfer (Thread 1)

```
For each large file:
  │
  ▼
  Is this file in large_files_done (progress)?
  YES → skip
  NO  ↓
  │
  rclone copyto SOURCE/Folder/file.mp4 DESTINATION/Folder/file.mp4
  │
  ▼
  ✅ Mark file complete in progress JSON
  ─── No local disk used! Server-side copy via rclone ───
```

### Normal Zip Pipeline (Thread 2) — THE CORE LOOP

```
Files: [file1, file2, ..., file5000]
Split into batches of 1000 (SPLIT_THRESHOLD):
  Batch 1: files 1-1000    → Part1.zip
  Batch 2: files 1001-2000 → Part2.zip
  ...

Each batch runs as a pipeline_worker (up to MAX_PARALLEL_WORKERS=2 at once):

┌─────────────────────────────────────────────────────────┐
│ pipeline_worker(batch, folder_path, s3_key, ...)        │
│                                                         │
│ 1. RESUME CHECK                                        │
│    Is this s3_key in completed_keys? → SKIP             │
│    Does this s3_key exist on S3? → SKIP                 │
│    Filter out completed_files from batch                │
│                                                         │
│ 2. SMART LOOP (handles splits)                          │
│    remaining_files = batch                              │
│    split_index = 0                                      │
│                                                         │
│    WHILE remaining_files > 0:                           │
│    │                                                    │
│    │  Create temp_dir (/content/temp_Part1_0_XXXX)      │
│    │  Write filelist.txt with remaining files            │
│    │                                                    │
│    │  ┌── DOWNLOAD ──────────────────────────────┐      │
│    │  │ rclone copy SOURCE/Folder temp_dir       │      │
│    │  │ --files-from filelist.txt                │      │
│    │  │ --transfers=6                            │      │
│    │  │                                          │      │
│    │  │ MONITOR LOOP (every 2 seconds):          │      │
│    │  │ ├── Check disk usage > 80%?              │      │
│    │  │ │   YES → proc.kill() → disk_triggered   │      │
│    │  │ ├── Check temp_dir size > 20GB?          │      │
│    │  │ │   YES → proc.kill() → size_triggered   │      │
│    │  │ └── Print: "DOWNLOADING 1234 MB / 20480" │      │
│    │  └──────────────────────────────────────────┘      │
│    │                                                    │
│    │  INVENTORY: Walk temp_dir, list downloaded files    │
│    │  remaining = files_not_downloaded_yet               │
│    │                                                    │
│    │  ┌── ZIP ───────────────────────────────────┐      │
│    │  │ Delete filelist.txt (not needed in zip)   │      │
│    │  │ cd temp_dir && zip -0 -r -q output.zip . │      │
│    │  │ (-0 = store mode, no compression)        │      │
│    │  └──────────────────────────────────────────┘      │
│    │                                                    │
│    │  ┌── UPLOAD TO S3 ─────────────────────────┐      │
│    │  │ s3.upload_file(output.zip, bucket, key)  │      │
│    │  │ mark_part_complete(folder, key, files)    │      │
│    │  │ → Saves progress JSON to S3              │      │
│    │  └──────────────────────────────────────────┘      │
│    │                                                    │
│    │  ┌── CLEANUP (in finally block) ───────────┐      │
│    │  │ Delete output.zip from local disk        │      │
│    │  │ shutil.rmtree(temp_dir)                  │  ◄── LOCAL FILES REMOVED
│    │  │ Kill any remaining rclone process         │      │
│    │  └──────────────────────────────────────────┘      │
│    │                                                    │
│    │  remaining_files > 0?                              │
│    │  YES → split_index++ → LOOP AGAIN (creates         │
│    │        Part1_Split1.zip, Part1_Split2.zip, etc.)   │
│    │  NO  → break (DONE with this batch)                │
│    │                                                    │
│    └── END WHILE ───────────────────────────────────────│
└─────────────────────────────────────────────────────────┘
```

### Disk Usage Timeline (Example: 1000 files, 15GB total)

```
Time ──────────────────────────────────────────────────────▶

Disk: ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

      ┌─ Download files ─┐┌ Zip ┐┌ Upload ┐┌ Clean ┐
      │ 0→15GB gradually ││15GB ││15GB zip││ 0GB   │
      └──────────────────┘└─────┘└────────┘└───────┘
                                                    ▲
                                              NEXT PART STARTS
                                              (disk is clean)
```

### What's stored on S3 after zipper runs

```
work_files_zips/
├── _index/folder_list.txt
├── Folder_A_List.txt
├── Folder_A_LargeFiles.json
├── Folder_A_Full.zip              ← if ≤1000 files total
│   OR
├── Folder_A_Part1.zip             ← files 1-1000
├── Folder_A_Part1_Split1.zip      ← if Part1 hit disk/size limit
├── Folder_A_Part2.zip             ← files 1001-2000
├── Folder_B_Part1.zip
├── _progress/
│   └── zipper_progress.json
└── ...
```

### Progress JSON Example

```json
{
  "Folder A": {
    "completed_keys": [
      "work_files_zips/Folder_A_Part1.zip",
      "work_files_zips/Folder_A_Part2.zip"
    ],
    "completed_files": ["Reports/Q1.pdf", "Reports/Q2.pdf", "..."],
    "large_files_done": ["BigVideo.mp4"],
    "folder_complete": true
  },
  "Folder B": {
    "completed_keys": ["work_files_zips/Folder_B_Part1.zip"],
    "completed_files": ["doc1.txt", "doc2.txt"],
    "large_files_done": [],
    "folder_complete": false
  }
}
```

---

## 3. PYTHON_UNZIPPER.PY — Download Zips, Unzip, Upload to Destination

### Purpose
Read zip files from S3, unzip them locally one at a time, upload the contents to DESTINATION, preserving the original folder structure.

### Step-by-Step Flow

```
START
  │
  ▼
┌─────────────────────────────────┐
│ 1. Install dependencies        │
│    apt-get install unzip        │
│    Install rclone if missing    │
│    Check rclone.conf exists     │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 2. Load progress from S3       │
│    unzipper_progress.json       │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────┐
│ 3. Fetch folder list from S3   │
│    _index/folder_list.txt       │
└──────────┬──────────────────────┘
           ▼
┌─────────────────────────────────────────────────────────┐
│ 4. Process folders in PARALLEL                          │
│    (ProcessPoolExecutor, max_workers=2)                 │
│                                                         │
│    Each folder runs as a process_folder() worker:       │
└──────────┬──────────────────────────────────────────────┘
           ▼
```

### process_folder() — One Folder's Flow

```
┌─────────────────────────────────────────────────────────┐
│ process_folder("Folder A")                              │
│                                                         │
│ 1. Is folder_complete? → SKIP                           │
│                                                         │
│ 2. List all zips on S3 for this folder:                 │
│    s3.list_objects(Prefix="work_files_zips/Folder_A_")  │
│    → [Folder_A_Part1.zip,                               │
│       Folder_A_Part1_Split1.zip,                        │
│       Folder_A_Part2.zip]                               │
│    Sorted naturally: Part1 → Part1_Split1 → Part2       │
│                                                         │
│ 3. Filter out already-processed keys (from progress)    │
│    If Part1.zip already in processed_keys → skip it     │
│                                                         │
│ 4. FOR EACH REMAINING ZIP (sequentially, one at a time):│
│    │                                                    │
│    │  ┌── download_unzip_upload_one() ──────────┐      │
│    │  │                                          │      │
│    │  │  STEP 1: Download zip from S3            │      │
│    │  │  s3.download_file(key, local_zip)         │      │
│    │  │  → /content/Folder_A_Part1.zip_XXXX      │      │
│    │  │                                          │      │
│    │  │  STEP 2: Unzip                           │      │
│    │  │  unzip -o -q local_zip -d temp_unzip/    │      │
│    │  │  → temp_unzip/Reports/Q1.pdf             │      │
│    │  │  → temp_unzip/Contracts/deal.docx        │      │
│    │  │                                          │      │
│    │  │  STEP 3: DELETE ZIP IMMEDIATELY           │  ◄── FREE DISK
│    │  │  os.remove(local_zip)                    │      │
│    │  │                                          │      │
│    │  │  STEP 4: Upload to DESTINATION           │      │
│    │  │  rclone copy temp_unzip/                 │      │
│    │  │    DESTINATION/Folder A/                  │      │
│    │  │    --transfers=6                         │      │
│    │  │                                          │      │
│    │  │  (rclone copy is ADDITIVE — only adds    │      │
│    │  │   new files, never deletes existing)     │      │
│    │  │                                          │      │
│    │  │  STEP 5: Save progress to S3             │      │
│    │  │  mark_zip_processed(folder, s3_key)       │      │
│    │  │                                          │      │
│    │  │  STEP 6: CLEANUP (in finally block)       │  ◄── FREE DISK
│    │  │  Delete local_zip (if still exists)       │      │
│    │  │  shutil.rmtree(temp_unzip/)              │      │
│    │  │                                          │      │
│    │  └──────────────────────────────────────────┘      │
│    │                                                    │
│    │  Success? Continue to next zip                     │
│    │  Failure? Log warning, continue anyway             │
│    │                                                    │
│    └── NEXT ZIP ────────────────────────────────────────│
│                                                         │
│ 5. All zips done → mark_folder_complete()               │
└─────────────────────────────────────────────────────────┘
```

### Unzipper Disk Usage Timeline (3 zips: 10GB, 8GB, 5GB)

```
Time ──────────────────────────────────────────────────────────────────────▶

      ZIP 1 (10GB)                ZIP 2 (8GB)                ZIP 3 (5GB)
      ┌─DL──┐┌Unzip┐┌Upload┐┌Cl┐ ┌─DL──┐┌Unzip┐┌Upload┐┌Cl┐ ┌DL┐┌Uz┐┌Up┐┌Cl┐
Disk: 0→10GB│10→20 │  20GB │ 0 │ 0→8GB│8→16  │  16GB │ 0 │ 0→5│5→10│10│ 0│
      └─────┘└─────┘└──────┘└──┘ └────┘└─────┘└──────┘└──┘ └──┘└──┘└──┘└─┘
                              ▲                         ▲                 ▲
                         DISK CLEANED              DISK CLEANED      DISK CLEANED
                         Progress saved            Progress saved    Progress saved
```

> **Key insight:** Disk usage goes up and down like a heartbeat. Each zip is fully processed and cleaned before the next one starts. Maximum disk used = 2× largest zip (download + unzipped content).

### What happens when large files already exist on DESTINATION

```
DESTINATION/Folder A/
├── BigVideo.mp4          ← 30GB, placed here by zipper's direct transfer

After unzipper runs:
DESTINATION/Folder A/
├── BigVideo.mp4          ← UNTOUCHED (rclone copy skips existing files)
├── Reports/Q1.pdf        ← added from Part1.zip
├── Reports/Q2.pdf        ← added from Part1.zip
├── Contracts/deal.docx   ← added from Part2.zip
└── ...
```

---

## Resume Scenarios

### Scenario 1: Colab crashes during zipper (Part2 downloading)

```
Before crash:
  Part1.zip ✅ uploaded to S3, progress saved
  Part2.zip ⏳ downloading (50% done)

On restart:
  Load progress → Part1 done, Part2 not done
  Skip Part1 → resume Part2 from scratch
  (files from Part1 already in completed_files, won't be re-downloaded)
```

### Scenario 2: Colab crashes during unzipper (uploading Part1 contents)

```
Before crash:
  Part1.zip downloaded ✅ unzipped ✅ uploading ⏳ (crashed mid-upload)

On restart:
  Load progress → Part1 NOT in processed_keys (wasn't marked complete)
  Re-download Part1.zip → re-unzip → re-upload
  (rclone copy is additive: files already uploaded are skipped automatically)
```

### Scenario 3: Zipper hits disk limit at 80%

```
Downloading 1000 files for Part1...
  800 files downloaded (78% disk) ✅
  801st file... 81% disk → DISK FULL triggered
  proc.kill() stops rclone

  Inventory: 800 files downloaded
  Zip those 800 → upload as Part1.zip
  Remaining 200 files → loop continues → Part1_Split1.zip
  Cleanup → disk back to normal → process Split1
```

### Scenario 4: Download exceeds 20GB zip limit

```
Downloading 1000 files for Part1...
  File sizes vary: some are 500MB each
  After 600 files → 21GB on disk → SIZE CAP triggered
  proc.kill() stops rclone

  Inventory: 600 files downloaded
  Zip those 600 → upload as Part1.zip (≤20GB zip)
  Remaining 400 files → loop continues → Part1_Split1.zip
```

---

## Configuration Reference

| Variable | In | Default | Description |
|---|---|---|---|
| `SOURCE` | mapper, zipper | `"onedrive:Work Files"` | rclone remote:path to read from |
| `DESTINATION` | zipper, unzipper | `"gdrive:Work Files"` | rclone remote:path to write to |
| `S3_BUCKET` | all 3 | `"workfiles123"` | S3/Wasabi bucket name |
| `S3_PREFIX` | all 3 | `"work_files_zips/"` | Prefix for all S3 keys |
| `AWS_ACCESS_KEY` | all 3 | `""` | S3 access key |
| `AWS_SECRET_KEY` | all 3 | `""` | S3 secret key |
| `S3_ENDPOINT` | all 3 | wasabi URL | S3 endpoint URL |
| `LARGE_FILE_THRESHOLD_GB` | mapper | `20` | Files above this → direct transfer |
| `MAX_ZIP_SIZE_GB` | zipper | `20` | Max zip size before splitting |
| `SPLIT_THRESHOLD` | zipper | `1000` | Files per batch/zip |
| `MAX_PARALLEL_WORKERS` | zipper, unzipper | `2` | Parallel workers |
| `DOWNLOAD_THREADS` | zipper | `6` | rclone --transfers for download |
| `UPLOAD_THREADS` | unzipper | `6` | rclone --transfers for upload |
| `DISK_LIMIT_PERCENT` | zipper, unzipper | `80` | Trigger split at this % |
| `SKIP_UPLOAD` | unzipper | `False` | True = extract locally only |

---

## Execution Order

```bash
# Step 1: Run mapper (once — scans SOURCE, creates file lists on S3)
python mapper.py

# Step 2: Run zipper (downloads from SOURCE, zips, uploads to S3)
# + directly transfers large files to DESTINATION
python python_zipper.py

# Step 3: Run unzipper (downloads zips from S3, unzips, uploads to DESTINATION)
python python_unzipper.py
```

All three scripts are **idempotent** — safe to run multiple times. They skip completed work automatically.

---

## Detailed Example: Splitting 29,566 Scattered Files

If you have a folder `Work Files/Archive` with **29,566 files** scattered across hundreds of nested subfolders, here is exactly how the system handles it:

### 1. The Mapping (Logical View)
`mapper.py` ignores the folder structure during scanning and produces a **flat list** of relative paths.
- **Total files:** 29,566
- **Threshold:** 1,000 files per batch
- **Result:** 30 logical batches are planned.
  - `Part1`: Files 1 to 1,000
  - `Part2`: Files 1,001 to 2,000
  - ...
  - `Part30`: Files 29,001 to 29,566

### 2. The Zipping (Physical View)
The `zipper.py` takes these batches and processes them. But it also respects **size limits** (20GB) and **disk space**.

**Case A: Small Files (Ideal)**
If each batch of 1,000 files is small (e.g., 2GB total), you get exactly 30 zip files on S3:
`Archive_Part1.zip`, `Archive_Part2.zip`, ..., `Archive_Part30.zip`.

**Case B: Large/Scattered Files (Reality)**
If `Part1` contains 1,000 files, but after downloading the first **400 files** the local folder hits the **20GB Size Cap**:
1. Zipper stops downloading.
2. It zips those 400 files and uploads as `Archive_Part1.zip`.
3. It keeps the remaining 600 files in the queue for the same batch.
4. It downloads the next batch of files from the remaining 600, hits the limit again, and uploads `Archive_Part1_Split1.zip`.
5. It finishes the last subset as `Archive_Part1_Split2.zip`.

### 3. The Unzipping (Reconstruction)
The `unzipper.py` reconstructs everything perfectly because:
- `unzip` restores the **full nested path** stored inside each zip (e.g., `Deeply/Nested/Folder/File.pdf`).
- `rclone copy` merges the contents of all zips (`Part1`, `Part1_Split1`, `Part2`, etc.) into the **same destination path**.

**Result on Destination:**
One single `Archive/` folder with all 29,566 files in their original nested subfolders, exactly as they were on the source.

---

## Deep Dive: Why Merging Works (Folder Preservation)

You might wonder: *If I have 10 zip parts, and each has its own "Part1" name, how do they merge back into one folder?*

The secret is in **Relative Paths** and **Additive Copies**.

### 1. Relative Paths inside the Zip
When `zipper.py` creates a zip (e.g., `Archive_Part1.zip`), it uses the `-r` (recursive) flag. This stores the **entire subfolder path** relative to the folder being zipped.

Imagine `Archive/` contains:
- `2024/Photos/A.jpg`
- `2024/Photos/B.jpg`

If `A.jpg` is in `Part1.zip` and `B.jpg` is in `Part2.zip`, the internal structure looks like this:

**Part1.zip Internal Structure:**
```
Archive_Part1.zip
└── 2024/
    └── Photos/
        └── A.jpg
```

**Part2.zip Internal Structure:**
```
Archive_Part2.zip
└── 2024/
    └── Photos/
        └── B.jpg
```

### 2. The Unzipping Step
When `unzipper.py` extracts `Part1.zip` to `temp_unzip/`:
`temp_unzip/` now contains `2024/Photos/A.jpg`.

### 3. The "Additive" Rclone Copy
The command `rclone copy temp_unzip/ DESTINATION/` is the magic step.
1. It looks at the destination.
2. It sees `2024/Photos/` doesn't exist yet (if this is the first part).
3. It creates the folder and uploads `A.jpg`.

Now, when `Part2.zip` is processed:
1. `temp_unzip/` is cleaned and now contains `2024/Photos/B.jpg`.
2. `rclone copy` looks at the destination.
3. It sees `2024/Photos/` **already exists**.
4. It **merges** the contents: it adds `B.jpg` into the existing `Photos/` folder.

**Final Result on Destination:**
```
DESTINATION/
└── Archive/
    └── 2024/
        └── Photos/
            ├── A.jpg (from Part 1)
            └── B.jpg (from Part 2)
```

**Conclusion:** The names of the zip files (`Part1_Split1`, etc.) are just containers for transport. The **data inside them** knows its original home (its path), and `rclone` ensures they all move back into that same home at the destination.
