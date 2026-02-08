import subprocess
import concurrent.futures
import os
import shutil
import argparse
import json
import threading
import queue
import sys
from tqdm import tqdm
from platformdirs import user_config_dir

# --- Import refactored processing functions ---
try:
    from . import processing
except ImportError:
    # Fallback for running file directly
    import processing

# --- Constants for Configuration ---
APP_NAME = "eac3-transcode"
APP_AUTHOR = "eac3-transcode"
CONFIG_FILENAME = "options.json"


# Worker initializer to assign a unique position to each worker's progress bar
def worker_init(worker_id_queue):
    threading.current_thread().worker_id = worker_id_queue.get()


def main():
    # --- GUI LAUNCHER ---
    # Check for --launch-gui *before* parsing args
    if "--launch-gui" in sys.argv:
        print("Launching GUI...")
        try:
            from . import gui
            gui.launch()
        except ImportError as e:
            print(f"🚨 Error: GUI dependencies are not installed. {e}", file=sys.stderr)
            print("Please run: pip install surround-to-eac3[gui]", file=sys.stderr)
        except Exception as e:
            # Catch other GUI-related errors (e.g., display not found)
            print(f"🚨 Error launching GUI: {e}", file=sys.stderr)
        sys.exit() # Exit after launching or failing
    # ---------------------

    # Initial check for ffmpeg and ffprobe
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        missing_tools = []
        if not shutil.which("ffmpeg"): missing_tools.append("ffmpeg")
        if not shutil.which("ffprobe"): missing_tools.append("ffprobe")
        print(f"🚨 Error: {', '.join(missing_tools)} is not installed or not found in your system's PATH. Please install ffmpeg.")
        return

    parser = argparse.ArgumentParser(
        description="Advanced video transcoder: E-AC3 for specific audio, language filtering, folder processing.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Add the new --launch-gui argument
    parser.add_argument(
        "--launch-gui",
        action="store_true",
        help="Launch the graphical user interface."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Path to the input video file or folder.",
        dest="input_path"
    )
    # ... (all your other arguments: -o, -br, -l, -j, --dry-run, --force-reprocess) ...
    parser.add_argument(
        "-o", "--outdir",
        help="Optional. Base directory to save processed files.\n"
             "If input is a folder, source structure is replicated under this directory.\n"
             "If not set, processed files are saved alongside originals.",
        dest="output_directory_base",
        default=None
    )
    parser.add_argument(
        "-br", "--bitrate",
        help="Audio bitrate for E-AC3 (e.g., '640k', '1536k'). Defaults to '1536k'.",
        dest="audio_bitrate",
        default="1536k"
    )
    parser.add_argument(
        "-l", "--langs",
        help="Comma-separated list of 3-letter audio languages to keep (e.g., 'eng,jpn').\nDefaults to 'eng,jpn'.",
        dest="languages",
        default="eng,jpn"
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int,
        default=os.cpu_count(), # Default to the number of CPU cores
        help=f"Number of files to process in parallel. Defaults to the number of CPU cores on your system ({os.cpu_count()})."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true", # Makes it a flag, e.g., --dry-run
        help="Analyze files and report actions without executing ffmpeg."
    )
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Force reprocessing of all files, even if an output file with the target name already exists."
    )
    parser.add_argument(
        "--log-commands",
        action="store_true",
        help="Log the full ffmpeg commands used for transcoding."
    )
    
    # --- Configuration File Logic (unchanged) ---
    config = {}
    user_config_dir_path = user_config_dir(APP_NAME, APP_AUTHOR)
    user_config_file_path = os.path.join(user_config_dir_path, CONFIG_FILENAME)

    if not os.path.exists(user_config_file_path):
        try:
            defaults = {action.dest: action.default for action in parser._actions if action.dest != "help" and not action.required and action.dest != "launch_gui"}
            os.makedirs(user_config_dir_path, exist_ok=True)
            with open(user_config_file_path, 'w') as f:
                json.dump(defaults, f, indent=4)
            print(f"✅ Created default configuration at: {user_config_file_path}")
        except Exception as e:
            print(f"⚠️ Warning: Could not create default config at '{user_config_file_path}': {e}")

    potential_paths = [os.path.join(os.getcwd(), CONFIG_FILENAME), user_config_file_path]
    loaded_config_path = None
    for path in potential_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    config = json.load(f)
                loaded_config_path = path
                break
            except (json.JSONDecodeError, IOError) as e:
                print(f"⚠️ Warning: Could not read or parse config at '{path}': {e}")
                break
    
    parser.set_defaults(**config)
    
    # Check for --input manually since it's no longer required by argparse
    # to allow --launch-gui to work without it.
    args = parser.parse_args()
    if not args.input_path:
        parser.error("-i/--input is required for CLI mode.")

    if loaded_config_path:
        print(f"✅ Loaded configuration from: {loaded_config_path}")

    if args.dry_run:
        print("--- DRY RUN MODE ENABLED: No files will be modified. ---")

    # --- File Discovery (unchanged) ---
    input_path_abs = os.path.abspath(args.input_path)
    files_to_process_paths = []
    if os.path.isdir(input_path_abs):
        print(f"📁 Scanning folder: {input_path_abs}")
        for root, _, filenames in os.walk(input_path_abs):
            for filename in filenames:
                if filename.lower().endswith(processing.SUPPORTED_EXTENSIONS):
                    files_to_process_paths.append(os.path.join(root, filename))
        if not files_to_process_paths:
            print("      No .mkv or .mp4 files found in the specified folder.")
    elif os.path.isfile(input_path_abs):
        if input_path_abs.lower().endswith(processing.SUPPORTED_EXTENSIONS):
            files_to_process_paths.append(input_path_abs)
        else:
            print(f"⚠️ Provided file '{args.input_path}' is not an .mkv or .mp4 file. Skipping this input.")
    else:
        print(f"🚨 Error: Input path '{args.input_path}' is not a valid file or directory.")
        return

    if not files_to_process_paths:
        print("No files to process.")
        return

    print(f"\nFound {len(files_to_process_paths)} file(s) to potentially process...")
    stats = {
        "processed": 0, "skipped_no_ops": 0, "skipped_no_transcode": 0,
        "skipped_identical_path": 0, "skipped_existing": 0, "failed": 0
    }

    # --- Main Processing Loop ---
    # We create the lock and queue here for the CLI job
    tqdm_lock = threading.Lock()
    worker_id_queue = queue.Queue()
    num_jobs = min(args.jobs, len(files_to_process_paths))
    for i in range(num_jobs):
        worker_id_queue.put(i + 1)

    try:
        with tqdm(total=len(files_to_process_paths), desc="Overall Progress", unit="file", ncols=100, smoothing=0.1, position=0, leave=True, file=sys.stderr) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_jobs, initializer=worker_init, initargs=(worker_id_queue,)) as executor:

                def submit_task(filepath):
                    """Wrapper to pass correct params to the processing function."""
                    worker_id = threading.current_thread().worker_id
                    # We pass the lock and the standard sys.stderr writer
                    return processing.process_single_file(
                        filepath, worker_id, args, input_path_abs,
                        tqdm_lock, sys.stderr
                    )

                future_to_path = {executor.submit(submit_task, path): path for path in files_to_process_paths}

                for future in concurrent.futures.as_completed(future_to_path):
                    path = future_to_path[future]
                    try:
                        status = future.result() 
                        if status in stats:
                            stats[status] += 1
                        else:
                            stats["failed"] += 1 
                            with tqdm_lock:
                                tqdm.write(f"🚨 UNKNOWN STATUS '{status}' for '{os.path.basename(path)}'.", file=sys.stderr)
                    except Exception as exc:
                        with tqdm_lock:
                             tqdm.write(f"🚨 CRITICAL ERROR during task for '{os.path.basename(path)}': {exc}", file=sys.stderr)
                        stats["failed"] += 1
                    finally:
                        pbar.update(1)

    except KeyboardInterrupt:
        print("\n\n🚨 Process interrupted by user. Shutting down gracefully...")
        return

    # --- Summary (unchanged) ---
    summary_title = "--- Dry Run Summary ---" if args.dry_run else "--- Processing Summary ---"
    processed_label = "Would be processed" if args.dry_run else "Successfully processed"
    
    print()
    print(f"\n{summary_title}")
    print(f"Total files checked: {len(files_to_process_paths)}")
    print(f"✅ {processed_label}: {stats['processed']}")
    total_skipped = stats['skipped_no_ops'] + stats['skipped_no_transcode'] + stats['skipped_identical_path'] + stats['skipped_existing']
    print(f"⏭️ Total Skipped: {total_skipped}")
    if total_skipped > 0:
        print(f"      - No target audio operations: {stats['skipped_no_ops']}")
        print(f"      - No transcoding required (all copy): {stats['skipped_no_transcode']}")
        print(f"      - Identical input/output path: {stats['skipped_identical_path']}")
        print(f"      - Output file already exists: {stats['skipped_existing']}")
    print(f"🚨 Failed to process: {stats['failed']}")
    print("--------------------------")

if __name__ == "__main__":
    main()
