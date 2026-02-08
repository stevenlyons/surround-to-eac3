import customtkinter as ctk
import threading
import sys
import os
import queue
import concurrent.futures
import argparse
import shutil
from tkinter import filedialog
from tqdm import tqdm
import json
from platformdirs import user_config_dir

# Import the processing functions from our new module
try:
    from . import processing
except ImportError:
    # Fallback for running file directly
    import processing

# --- Constants ---
APP_NAME = "eac3-transcode"
APP_AUTHOR = "eac3-transcode"
CONFIG_FILENAME = "options.json"

# --- Worker Initializer (needed for GUI thread pool) ---
def worker_init(worker_id_queue):
    """Assigns a unique ID to each worker thread for its progress bar."""
    threading.current_thread().worker_id = worker_id_queue.get()


class GuiLogger:
    """A file-like object to redirect stdout/stderr to the GUI text box."""
    def __init__(self, app, textbox):
        self.app = app
        self.textbox = textbox

    def write(self, msg):
        """Write message to the textbox, ensuring it's thread-safe."""
        
        def _write_to_box():
            """Internal function to run on the main thread."""
            self.textbox.configure(state="normal")
            self.textbox.insert("end", str(msg))
            self.textbox.see("end") # Auto-scroll
            self.textbox.configure(state="disabled")

        # Use app.after to schedule the GUI update on the main thread
        self.app.after(0, _write_to_box)

    def flush(self):
        """Required for file-like object interface."""
        pass


class TranscoderApp(ctk.CTk):
    """Main GUI application window."""
    
    def __init__(self):
        super().__init__()

        self.title("E-AC3 Transcoder")
        self.geometry("800x600")
        ctk.set_appearance_mode("system")
        
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=0)

        # --- Load Config File ---
        default_config = self.load_default_config()

        # --- Main Frame ---
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        self.main_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(1, weight=1) # Log box row

        # --- Options Frame ---
        self.options_frame = ctk.CTkFrame(self.main_frame)
        self.options_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        self.options_frame.grid_columnconfigure(1, weight=1)
        
        # --- Log Frame ---
        self.log_frame = ctk.CTkFrame(self.main_frame)
        self.log_frame.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="nsew")
        self.log_frame.grid_columnconfigure(0, weight=1)
        self.log_frame.grid_rowconfigure(0, weight=1)

        # --- Button Frame ---
        self.button_frame = ctk.CTkFrame(self)
        self.button_frame.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="ew")
        self.button_frame.grid_columnconfigure(0, weight=1)

        # --- Widgets: Options ---
        # Input Path
        self.input_label = ctk.CTkLabel(self.options_frame, text="Input Path:")
        self.input_label.grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.input_entry = ctk.CTkEntry(self.options_frame, placeholder_text="Select a file or folder...")
        self.input_entry.grid(row=0, column=1, padx=(0, 5), pady=5, sticky="ew")
        self.input_file_button = ctk.CTkButton(self.options_frame, text="File...", width=80, command=self.select_input_file)
        self.input_file_button.grid(row=0, column=2, padx=5, pady=5)
        self.input_folder_button = ctk.CTkButton(self.options_frame, text="Folder...", width=80, command=self.select_input_folder)
        self.input_folder_button.grid(row=0, column=3, padx=(0, 10), pady=5)

        # Output Path
        self.output_label = ctk.CTkLabel(self.options_frame, text="Output Dir:")
        self.output_label.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.output_entry = ctk.CTkEntry(self.options_frame, placeholder_text="Optional (defaults to same as input)")
        self.output_entry.grid(row=1, column=1, padx=(0, 5), pady=5, sticky="ew")
        self.output_folder_button = ctk.CTkButton(self.options_frame, text="Select...", width=80, command=self.select_output_folder)
        self.output_folder_button.grid(row=1, column=2, columnspan=2, padx=(0, 10), pady=5, sticky="ew")

        # Bitrate
        self.bitrate_label = ctk.CTkLabel(self.options_frame, text="Bitrate:")
        self.bitrate_label.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.bitrate_entry = ctk.CTkEntry(self.options_frame)
        self.bitrate_entry.grid(row=2, column=1, padx=(0, 10), pady=5, sticky="w")
        
        # Languages
        self.langs_label = ctk.CTkLabel(self.options_frame, text="Languages:")
        self.langs_label.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.langs_entry = ctk.CTkEntry(self.options_frame)
        self.langs_entry.grid(row=3, column=1, padx=(0, 10), pady=5, sticky="w")

        # Jobs
        self.jobs_label = ctk.CTkLabel(self.options_frame, text=f"Jobs (CPUs: {os.cpu_count()}):")
        self.jobs_label.grid(row=4, column=0, padx=10, pady=5, sticky="w")
        self.jobs_slider = ctk.CTkSlider(self.options_frame, from_=1, to=os.cpu_count(), number_of_steps=os.cpu_count() - 1, command=lambda v: self.jobs_value_label.configure(text=int(v)))
        self.jobs_slider.grid(row=4, column=1, padx=(0, 10), pady=5, sticky="ew")
        self.jobs_value_label = ctk.CTkLabel(self.options_frame, text=os.cpu_count(), width=30)
        self.jobs_value_label.grid(row=4, column=2, padx=(0, 10), pady=5)

        # Checkboxes
        self.dry_run_var = ctk.IntVar()
        self.dry_run_check = ctk.CTkCheckBox(self.options_frame, text="Dry Run (Analyze only)", variable=self.dry_run_var)
        self.dry_run_check.grid(row=5, column=0, padx=10, pady=10, sticky="w")
        
        self.force_reprocess_var = ctk.IntVar()
        self.force_reprocess_check = ctk.CTkCheckBox(self.options_frame, text="Force Reprocess (Overwrite existing)", variable=self.force_reprocess_var)
        self.force_reprocess_check.grid(row=5, column=1, padx=10, pady=10, sticky="w")

        self.downmix_var = ctk.IntVar()
        self.downmix_check = ctk.CTkCheckBox(self.options_frame, text="Downmix 5.1 to Stereo (add 2ch AAC track)", variable=self.downmix_var)
        self.downmix_check.grid(row=6, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="w")

        # Load Config Button
        self.load_config_button = ctk.CTkButton(self.options_frame, text="Load Config...", width=80, command=self.load_config_from_file)
        self.load_config_button.grid(row=6, column=3, padx=(0, 10), pady=(0, 10), sticky="e")


        # --- Widgets: Log ---
        self.log_textbox = ctk.CTkTextbox(self.log_frame, state="disabled", font=("Courier New", 12))
        self.log_textbox.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")

        # --- Widgets: Buttons ---
        self.start_button = ctk.CTkButton(self.button_frame, text="Start Processing", height=40, command=self.start_processing)
        self.start_button.grid(row=0, column=0, padx=10, pady=5, sticky="ew")

        # --- Member Variables ---
        self.processing_thread = None

        # --- Apply Initial Config ---
        self.apply_config(default_config)

    # --- Config Loader ---
    def load_default_config(self) -> dict:
        """Loads default config from file, mimicking main.py logic."""
        user_config_dir_path = user_config_dir(APP_NAME, APP_AUTHOR)
        user_config_file_path = os.path.join(user_config_dir_path, CONFIG_FILENAME)
        
        potential_paths = [os.path.join(os.getcwd(), CONFIG_FILENAME), user_config_file_path]
        config = {}
        
        for path in potential_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        config = json.load(f)
                    # We found the config, stop looking
                    break 
                except (json.JSONDecodeError, IOError):
                    # Config is corrupt, just use defaults
                    break
        return config

    def load_config_from_file(self):
        """Opens a dialog to load a config .json file and applies it."""
        path = filedialog.askopenfilename(
            title="Load Config File",
            filetypes=[("JSON files", "*.json"), ("All Files", "*.*")]
        )
        if not path:
            return # User cancelled

        try:
            with open(path, 'r') as f:
                config = json.load(f)
            self.apply_config(config)
            
            # Log success
            self.log_textbox.configure(state="normal")
            self.log_textbox.insert("1.0", f"✅ Successfully loaded config from: {os.path.basename(path)}\n\n")
            self.log_textbox.configure(state="disabled")

        except (json.JSONDecodeError, IOError, Exception) as e:
            # Log failure
            self.log_textbox.configure(state="normal")
            self.log_textbox.insert("1.0", f"🚨 Error loading config: {e}\n\n")
            self.log_textbox.configure(state="disabled")

    def apply_config(self, config: dict):
        """Applies a config dictionary to all the GUI fields."""
        
        # Bitrate
        self.bitrate_entry.delete(0, "end")
        self.bitrate_entry.insert(0, config.get("audio_bitrate", "1536k"))
        
        # Languages
        self.langs_entry.delete(0, "end")
        self.langs_entry.insert(0, config.get("languages", "eng,jpn"))

        # Jobs
        default_jobs = config.get("jobs", os.cpu_count())
        self.jobs_slider.set(default_jobs)
        self.jobs_value_label.configure(text=default_jobs)

        # Checkboxes
        self.dry_run_var.set(config.get("dry_run", 0))
        self.force_reprocess_var.set(config.get("force_reprocess", 0))
        self.downmix_var.set(config.get("downmix", 0))

    # --- Button Callbacks ---
    def select_input_file(self):
        path = filedialog.askopenfilename(filetypes=[("Video Files", "*.mkv *.mp4"), ("All Files", "*.*")])
        if path:
            self.input_entry.delete(0, "end")
            self.input_entry.insert(0, path)

    def select_input_folder(self):
        path = filedialog.askdirectory()
        if path:
            self.input_entry.delete(0, "end")
            self.input_entry.insert(0, path)

    def select_output_folder(self):
        path = filedialog.askdirectory()
        if path:
            self.output_entry.delete(0, "end")
            self.output_entry.insert(0, path)

    # --- Processing Logic ---
    def start_processing(self):
        """Starts the transcoding job in a new thread."""
        input_path = self.input_entry.get()
        if not input_path:
            self.log_textbox.configure(state="normal")
            self.log_textbox.delete("1.0", "end")
            self.log_textbox.insert("end", "🚨 Error: Please select an input file or folder first.")
            self.log_textbox.configure(state="disabled")
            return

        # Disable button, clear log
        self.start_button.configure(state="disabled", text="Processing...")
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        self.log_textbox.configure(state="disabled")
        
        # Start the job in a separate thread to keep the GUI responsive
        self.processing_thread = threading.Thread(target=self.run_processing_job, daemon=True)
        self.processing_thread.start()

    def run_processing_job(self):
        """
        THE CORE PROCESSING LOOP - This runs on a worker thread.
        It mimics the logic from `main.py` but uses the GUI logger.
        """
        
        # 1. Create a logger that writes to our GUI
        gui_logger = GuiLogger(self, self.log_textbox)
        
        # 2. Gather settings from GUI into a mock 'args' object
        mock_args = argparse.Namespace(
            input_path=self.input_entry.get(),
            output_directory_base=self.output_entry.get() or None,
            audio_bitrate=self.bitrate_entry.get(),
            languages=self.langs_entry.get(),
            jobs=int(self.jobs_slider.get()),
            dry_run=bool(self.dry_run_var.get()),
            force_reprocess=bool(self.force_reprocess_var.get()),
            downmix=bool(self.downmix_var.get())
        )

        # 3. Setup locks and queues for this job
        tqdm_lock = threading.Lock()
        worker_id_queue = queue.Queue()

        # 4. File Discovery (mirrors main.py)
        try:
            input_path_abs = os.path.abspath(mock_args.input_path)
            files_to_process_paths = []
            
            if os.path.isdir(input_path_abs):
                gui_logger.write(f"📁 Scanning folder: {input_path_abs}\n")
                for root, _, filenames in os.walk(input_path_abs):
                    for filename in filenames:
                        if filename.lower().endswith(processing.SUPPORTED_EXTENSIONS):
                            files_to_process_paths.append(os.path.join(root, filename))
                if not files_to_process_paths:
                    gui_logger.write("      No .mkv or .mp4 files found.\n")
            elif os.path.isfile(input_path_abs):
                if input_path_abs.lower().endswith(processing.SUPPORTED_EXTENSIONS):
                    files_to_process_paths.append(input_path_abs)
                else:
                    gui_logger.write(f"⚠️ Provided file is not an .mkv or .mp4.\n")
            else:
                gui_logger.write(f"🚨 Error: Input path is not a valid file or directory.\n")
                self.processing_finished()
                return

            if not files_to_process_paths:
                gui_logger.write("No files to process.\n")
                self.processing_finished()
                return

            gui_logger.write(f"\nFound {len(files_to_process_paths)} file(s) to potentially process...\n")
            
            stats = {
                "processed": 0, "skipped_no_ops": 0, "skipped_no_transcode": 0,
                "skipped_identical_path": 0, "skipped_existing": 0, "failed": 0
            }

            num_jobs = min(mock_args.jobs, len(files_to_process_paths))
            for i in range(num_jobs):
                worker_id_queue.put(i + 1) # TQDM positions 1, 2, 3...

            # 5. Run ThreadPoolExecutor (mirrors main.py)
            # The 'file=gui_logger' is the magic that redirects all tqdm output
            with tqdm(total=len(files_to_process_paths), desc="Overall Progress", unit="file", ncols=100, smoothing=0.1, position=0, leave=True, file=gui_logger) as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=num_jobs, initializer=worker_init, initargs=(worker_id_queue,)) as executor:

                    def submit_task(filepath):
                        """Wrapper to pass correct params to the processing function."""
                        worker_id = threading.current_thread().worker_id
                        return processing.process_single_file(
                            filepath, worker_id, mock_args, input_path_abs,
                            tqdm_lock, gui_logger # Pass the lock and GUI logger
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
                                    tqdm.write(f"🚨 UNKNOWN STATUS '{status}' for '{os.path.basename(path)}'.\n", file=gui_logger)
                        except Exception as exc:
                            with tqdm_lock:
                                tqdm.write(f"🚨 CRITICAL ERROR during task for '{os.path.basename(path)}': {exc}\n", file=gui_logger)
                            stats["failed"] += 1
                        finally:
                            pbar.update(1)

            # 6. Print Summary (mirrors main.py)
            summary_title = "--- Dry Run Summary ---" if mock_args.dry_run else "--- Processing Summary ---"
            processed_label = "Would be processed" if mock_args.dry_run else "Successfully processed"
            
            summary = [
                f"\n\n{summary_title}\n",
                f"Total files checked: {len(files_to_process_paths)}\n",
                f"✅ {processed_label}: {stats['processed']}\n"
            ]
            
            total_skipped = stats['skipped_no_ops'] + stats['skipped_no_transcode'] + stats['skipped_identical_path'] + stats['skipped_existing']
            summary.append(f"⏭️ Total Skipped: {total_skipped}\n")
            
            if total_skipped > 0:
                summary.append(f"      - No target audio operations: {stats['skipped_no_ops']}\n")
                summary.append(f"      - No transcoding required (all copy): {stats['skipped_no_transcode']}\n")
                summary.append(f"      - Identical input/output path: {stats['skipped_identical_path']}\n")
                summary.append(f"      - Output file already exists: {stats['skipped_existing']}\n")
            
            summary.append(f"🚨 Failed to process: {stats['failed']}\n")
            summary.append("--------------------------\n")
            gui_logger.write("".join(summary))

        except Exception as e:
            gui_logger.write(f"\n\n🚨 A CRITICAL ERROR occurred: {e}\n")
        finally:
            # 7. Re-enable the button on the main thread
            self.processing_finished()

    def processing_finished(self):
        """Schedules the 'Start' button to be re-enabled on the main GUI thread."""
        # Use self.after, not self.app.after, as 'self' is the app instance
        self.after(0, lambda: self.start_button.configure(state="normal", text="Start Processing"))


def launch():
    """Entry point for launching the GUI."""
    # Check for ffmpeg/ffprobe before launching
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        ctk.set_appearance_mode("system")
        root = ctk.CTk()
        root.withdraw() # Hide the main window
        # Simple message box
        from tkinter import messagebox
        messagebox.showerror(
            "Missing Dependencies",
            "Error: ffmpeg and/or ffprobe are not installed or not found in your system's PATH. Please install ffmpeg to use this tool."
        )
        root.destroy()
        return

    app = TranscoderApp()
    app.mainloop()

