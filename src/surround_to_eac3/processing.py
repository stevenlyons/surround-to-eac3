import subprocess
import os
import shlex
import shutil
import json
import sys
from tqdm import tqdm

# --- Constants ---
SUPPORTED_EXTENSIONS = (".mkv", ".mp4")


def get_video_duration(filepath: str, log_commands: bool = False) -> tuple[float, list[str]]:
    """Gets the duration of a video file in seconds."""
    logs = []
    if not shutil.which("ffprobe"):
        return 0.0, logs

    command = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath
    ]
    if log_commands:
        logs.append(f"      📋 CMD: {shlex.join(command)}")
    try:
        process = subprocess.run(command, capture_output=True, text=True, check=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        return float(process.stdout.strip()), logs
    except (subprocess.CalledProcessError, ValueError):
        return 0.0, logs


def get_stream_info(filepath: str, stream_type: str = "audio", log_commands: bool = False) -> tuple[list[dict], list[str]]:
    """
    Retrieves details for specified stream types (audio, video, subtitle) in a file.
    """
    logs = []
    if not shutil.which("ffprobe"):
        logs.append(f"      ⚠️ Warning: ffprobe is missing. Cannot get {stream_type} stream info for '{os.path.basename(filepath)}'.")
        return [], logs

    select_streams_option = {
        "audio": "a",
        "video": "v",
        "subtitle": "s"
    }.get(stream_type, "a")

    ffprobe_cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", select_streams_option, filepath
    ]

    if log_commands:
        logs.append(f"      📋 CMD: {shlex.join(ffprobe_cmd)}")

    try:
        process = subprocess.run(
            ffprobe_cmd, capture_output=True, text=True, check=False,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )
        if process.returncode != 0:
            return [], logs
        if not process.stdout.strip():
            return [], logs

        data = json.loads(process.stdout)
        streams_details = []
        for stream in data.get("streams", []):
            detail = {
                "index": stream["index"],
                "codec_name": stream.get("codec_name", "unknown")
            }
            if stream_type == "audio":
                detail["channels"] = stream.get("channels")
                detail["language"] = stream.get("tags", {}).get("language", "und").lower()
            streams_details.append(detail)
        return streams_details, logs
    except json.JSONDecodeError:
        logs.append(f"      ⚠️ Warning: Failed to decode ffprobe JSON for {stream_type} streams in '{os.path.basename(filepath)}'.")
        return [], logs
    except Exception as e:
        logs.append(f"      ⚠️ Error getting {stream_type} stream info for '{os.path.basename(filepath)}': {e}")
        return [], logs
    

def process_file_with_ffmpeg(
    input_filepath: str,
    final_output_filepath: str | None,
    audio_bitrate: str,
    audio_processing_ops: list[dict],
    duration: float,
    pbar_position: int,
    tqdm_lock,
    tqdm_file_writer=sys.stderr,
    log_commands: bool = False
) -> tuple[bool, list[str]]:
    """
    Processes a single video file using ffmpeg, writing to a temporary file first.
    """
    logs = []
    if not shutil.which("ffmpeg"):
        logs.append("      🚨 Error: ffmpeg is not installed or not found.")
        return False, logs

    temp_output_filepath = final_output_filepath + ".tmp"
    base_filename = os.path.basename(input_filepath)
    output_filename = os.path.basename(final_output_filepath)

    ffmpeg_cmd = ["ffmpeg", "-nostdin", "-i", input_filepath, "-map_metadata", "0"]
    map_operations = []
    output_audio_stream_ffmpeg_idx = 0 

    map_operations.extend(["-map", "0:v?", "-c:v", "copy"])
    map_operations.extend(["-map", "0:s?", "-c:s", "copy"])

    for op_details in audio_processing_ops:
        map_operations.extend(["-map", f"0:{op_details['index']}"])
        if op_details['op'] == 'transcode':
            map_operations.extend([f"-c:a:{output_audio_stream_ffmpeg_idx}", "eac3", f"-b:a:{output_audio_stream_ffmpeg_idx}", audio_bitrate, f"-ac:a:{output_audio_stream_ffmpeg_idx}", "6", f"-metadata:s:a:{output_audio_stream_ffmpeg_idx}", f"language={op_details['lang']}"])
        elif op_details['op'] == 'copy':
            map_operations.extend([f"-c:a:{output_audio_stream_ffmpeg_idx}", "copy"])
        elif op_details['op'] == 'downmix':
            map_operations.extend([f"-c:a:{output_audio_stream_ffmpeg_idx}", "aac", f"-b:a:{output_audio_stream_ffmpeg_idx}", "256k", f"-ac:a:{output_audio_stream_ffmpeg_idx}", "2", f"-metadata:s:a:{output_audio_stream_ffmpeg_idx}", f"language={op_details['lang']}", f"-metadata:s:a:{output_audio_stream_ffmpeg_idx}", "title=Stereo", f"-disposition:a:{output_audio_stream_ffmpeg_idx}", "0"])
            if log_commands:
                logs.append(f"      📋 Downmix: stream #{op_details['index']} ({op_details['lang']}) -> stereo AAC @ 256k")
        output_audio_stream_ffmpeg_idx += 1
    
    ffmpeg_cmd.extend(map_operations)

    if final_output_filepath.lower().endswith('.mkv'):
        ffmpeg_cmd.extend(['-f', 'matroska'])
    elif final_output_filepath.lower().endswith('.mp4'):
        ffmpeg_cmd.extend(['-f', 'mp4'])

    ffmpeg_cmd.extend(["-y", "-v", "quiet", "-stats_period", "1", "-progress", "pipe:1", temp_output_filepath])

    logs.append(f"      ⚙️ Processing: '{base_filename}' -> '{output_filename}'")

    if log_commands:
        logs.append(f"      📋 CMD: {shlex.join(ffmpeg_cmd)}")

    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)

    file_pbar = None
    if duration > 0:
        file_pbar = tqdm(total=int(duration), desc=f"└─'{base_filename[:30]}…'", position=pbar_position, unit='s', leave=False, ncols=100, file=tqdm_file_writer)
    
    for line in process.stdout:
        if "out_time_us" in line:
            try:
                time_us = int(line.strip().split("=")[1])
                elapsed_seconds = time_us / 1_000_000
                if file_pbar: 
                    update_amount = max(0, elapsed_seconds - file_pbar.n)
                    if update_amount > 0:
                        file_pbar.update(update_amount)
            except (ValueError, IndexError):
                continue

    process.wait()
    if file_pbar:
        file_pbar.close()

    if process.returncode == 0:
        if os.path.exists(temp_output_filepath) and os.path.getsize(temp_output_filepath) > 0:
            os.rename(temp_output_filepath, final_output_filepath)
            logs.append(f"      ✅ Success: '{output_filename}' saved.")
            return True, logs
        else:
            logs.append(f"      ⚠️ Warning: ffmpeg reported success, but temp file is missing or empty.")
            if os.path.exists(temp_output_filepath):
                 os.remove(temp_output_filepath)
            return False, logs
    else:
        logs.append(f"      🚨 Error during ffmpeg processing for '{base_filename}'. RC: {process.returncode}")
        stderr_output = process.stderr.read()
        if stderr_output:
            logs.append(f"             ffmpeg stderr:\n{stderr_output.strip()}")
        return False, logs


def process_single_file(
    filepath: str, 
    pbar_position: int, 
    args: "argparse.Namespace", 
    input_path_abs: str,
    tqdm_lock,
    tqdm_file_writer=sys.stderr
) -> str:
    """
    Analyzes and processes a single file, managing temporary files for graceful exit.
    """
    file_specific_logs = []
    final_status = "failed"
    
    display_name = os.path.relpath(filepath, input_path_abs) if os.path.isdir(input_path_abs) else os.path.basename(filepath)
    file_specific_logs.append(f"▶️ Checked: '{display_name}'")
    
    target_languages = [lang.strip().lower() for lang in args.languages.split(',') if lang.strip()]

    log_commands = getattr(args, 'log_commands', False)

    audio_streams_details, get_info_logs = get_stream_info(filepath, "audio", log_commands=log_commands)
    file_specific_logs.extend(get_info_logs)
    
    audio_ops_for_ffmpeg = []
    if not audio_streams_details:
        file_specific_logs.append("      ℹ️ No audio streams found in this file.")
    else:
        for stream in audio_streams_details:
            lang = stream['language']
            op_to_perform = None
            channels_info = f"{stream.get('channels')}ch" if stream.get('channels') is not None else "N/Ach"
            codec_name = stream.get('codec_name', 'unknown')

            if lang in target_languages:
                is_5_1 = stream.get('channels') == 6
                is_not_ac3_eac3 = codec_name not in ['ac3', 'eac3']
                if is_5_1 and is_not_ac3_eac3:
                    op_to_perform = 'transcode'
                    file_specific_logs.append(f"      🔈 Will transcode: Audio stream #{stream['index']} ({lang}, {channels_info}, {codec_name})")
                else:
                    op_to_perform = 'copy'
                    reason_parts = [f"already {codec_name}" if codec_name in ['ac3', 'eac3'] else None, f"not 5.1 ({channels_info})" if stream.get('channels') != 6 else None]
                    reason = ", ".join(filter(None, reason_parts)) or "meets other criteria for copying"
                    file_specific_logs.append(f"      🔈 Will copy: Audio stream #{stream['index']} ({lang}, {channels_info}, {codec_name}) - Reason: {reason}")
            else:
                file_specific_logs.append(f"      🔈 Will drop: Audio stream #{stream['index']} ({lang}, {channels_info}, {codec_name}) - Not a target language.")

            if op_to_perform:
                audio_ops_for_ffmpeg.append({'index': stream['index'], 'op': op_to_perform, 'lang': lang})

    # Add stereo downmix ops for each 5.1 stream if --downmix is enabled
    downmix_enabled = getattr(args, 'downmix', False)
    if downmix_enabled:
        downmix_ops = []
        for op in audio_ops_for_ffmpeg:
            # Find the original stream to check channel count
            orig_stream = next((s for s in audio_streams_details if s['index'] == op['index']), None)
            if orig_stream and orig_stream.get('channels') == 6:
                downmix_ops.append({'index': op['index'], 'op': 'downmix', 'lang': op['lang']})
                file_specific_logs.append(f"      🔉 Will downmix: Audio stream #{op['index']} ({op['lang']}) -> stereo AAC")
        audio_ops_for_ffmpeg.extend(downmix_ops)

    if not audio_ops_for_ffmpeg:
        file_specific_logs.append(f"      ⏭️ Skipping '{display_name}': No target audio streams to process (copy/transcode).")
        with tqdm_lock:
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
        final_status = "skipped_no_ops"
        return final_status

    needs_processing = any(op['op'] in ('transcode', 'downmix') for op in audio_ops_for_ffmpeg)
    if not needs_processing:
        file_specific_logs.append(f"      ⏭️ Skipping '{display_name}': No transcoding required.")
        with tqdm_lock:
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
        final_status = "skipped_no_transcode"
        return final_status
    
    name, ext = os.path.splitext(os.path.basename(filepath))
    output_filename = f"{name}_eac3{ext}"
    output_dir_for_this_file = os.path.dirname(filepath) 
    if args.output_directory_base: 
        if os.path.isdir(input_path_abs):
            relative_dir = os.path.relpath(os.path.dirname(filepath), start=input_path_abs)
            output_dir_for_this_file = os.path.join(args.output_directory_base, relative_dir) if relative_dir != "." else args.output_directory_base
        else: 
            output_dir_for_this_file = args.output_directory_base
    
    final_output_filepath = os.path.join(output_dir_for_this_file, output_filename)

    if os.path.exists(final_output_filepath) and not args.force_reprocess:
        file_specific_logs.append(f"           ⏭️ Skipping: Output file already exists. Use --force-reprocess to override.")
        with tqdm_lock:
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
        final_status = "skipped_existing"
        return final_status
    
    if os.path.abspath(filepath) == os.path.abspath(final_output_filepath):
        file_specific_logs.append(f"      ⚠️ Warning: Input and output paths are identical. Skipping.")
        with tqdm_lock:
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
        final_status = "skipped_identical_path"
        return final_status
    
    if args.dry_run:
        file_specific_logs.append(f"      DRY RUN: Would process '{display_name}'. No changes will be made.")
        with tqdm_lock:
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
        final_status = "processed"
        return final_status

    if not os.path.isdir(output_dir_for_this_file):
        try:
            os.makedirs(output_dir_for_this_file, exist_ok=True)
        except OSError as e:
            file_specific_logs.append(f"      🚨 Error creating output directory '{output_dir_for_this_file}': {e}")
            with tqdm_lock:
                for log_msg in file_specific_logs:
                    tqdm.write(log_msg, file=tqdm_file_writer)
            return "failed"
            
    duration, duration_logs = get_video_duration(filepath, log_commands=log_commands)
    file_specific_logs.extend(duration_logs)
    if duration == 0:
        file_specific_logs.append(f"      ⚠️ Could not determine duration for '{display_name}'. Per-file progress will not be shown.")
    
    temp_filepath = final_output_filepath + ".tmp"
    try:
        success, ffmpeg_logs = process_file_with_ffmpeg(
            filepath, final_output_filepath, args.audio_bitrate,
            audio_ops_for_ffmpeg, duration, pbar_position,
            tqdm_lock, tqdm_file_writer,
            log_commands=log_commands
        )
        file_specific_logs.extend(ffmpeg_logs)
        final_status = "processed" if success else "failed"
    finally:
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError as e:
                file_specific_logs.append(f"      🚨 Error cleaning up temp file '{temp_filepath}': {e}")

        with tqdm_lock: 
            for log_msg in file_specific_logs:
                tqdm.write(log_msg, file=tqdm_file_writer)
    return final_status
