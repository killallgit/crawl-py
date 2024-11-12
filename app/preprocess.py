import os
import json
import hashlib
import subprocess
from tqdm import tqdm
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from pathlib import Path
from typing import List, Sequence, Union

def merge_directory_paths(directories: Sequence[Union[str, Path]]) -> List[Path]:
    supported_extensions=['.wav', '.mp3', '.flac', '.ogg', '.m4a']
    merged_paths = []
    
    for directory in directories:
        # Convert to Path object if not already
        dir_path = Path(directory)
        
        # Check if directory exists
        if not dir_path.is_dir():
            raise ValueError(f"Path {dir_path} is not a valid directory")
        
        # Recursively get all files and subdirectories
        merged_paths.extend(list(dir_path.rglob('*')))
    
    # Remove duplicates and sort (optional)
    merged_paths = sorted(set(merged_paths))
    # remove non audio files
    merged_paths = [p for p in merged_paths if p.suffix.lower() in supported_extensions]
    return merged_paths

def convert_audio_to_wav(input_path, output_path, target_sample_rate=16000, channels=1):
    """
    Convert audio file to mono 16kHz WAV using ffmpeg with more robust error handling
    """
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Construct ffmpeg command with more detailed error checking
        command = [
            'ffmpeg', 
            '-i', input_path, 
            '-ac', str(channels),  # convert to mono
            '-ar', str(target_sample_rate),  # set sample rate 
            '-acodec', 'pcm_s16le',  # use 16-bit PCM
            '-y',  # overwrite output file
            output_path
        ]
        
        # Run conversion with detailed error output
        result = subprocess.run(command, 
                                capture_output=True, 
                                text=True, 
                                check=True)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg error converting {input_path}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error converting {input_path}: {str(e)}")
        return False

def calculate_shasum(file_path):
    """
    Calculate SHA-256 hash of a file with error handling
    """
    try:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash for {file_path}: {str(e)}")
        return None

def sanitize_filename(filename):
    if filename.count('_') <= 3:
        # Split on the last underscore
        match = re.match(r'^(.+)_([^_]+)$', filename)
        if match:
            return match.group(1), match.group(2)
    
    # If more than 3 underscores or no match, return the original filename
    return filename, ''

def process_audio_files(raw_audio_paths, root_dataset_dir):
    dataset_data_dir = os.path.join(root_dataset_dir, 'data')
    # Create output directories if they don't exist
    os.makedirs(dataset_data_dir, exist_ok=True)
    
    # Load existing metadata
    metadata = []
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = [json.loads(line.strip()) for line in f if line.strip()]
    except Exception as e:
        logger.warning(f"Error reading existing metadata: {str(e)}")
    
    # Process files with progress bar
    for input_path in tqdm(raw_audio_paths, desc="Processing Audio Files"):
        try:
            # Input and output paths
            # Generate a unique, safe filename
            file_name, file_ext = input_path.name, input_path.suffix
            safe_id, original_title = sanitize_filename(file_name)
            output_path = os.path.join(root_dataset_dir, f"{safe_id}.wav")
            
            # Check if file already processed with matching hash
            existing_entry = next(
                (entry for entry in metadata 
                 if entry.get('file_name') == f"data/{safe_id}.wav"), 
                None
            )
            
            # Calculate current input file hash
            current_hash = calculate_shasum(input_path)
            if not current_hash:
                logger.warning(f"Skipping {input_path}: Could not calculate hash")
                continue
            
            # Skip if already processed with same hash
            if existing_entry and existing_entry.get('shasum') == current_hash:
                logger.info(f"Skipping {input_path}: Already processed")
                continue
            
            # Convert audio
            if convert_audio_to_wav(input_path, output_path):
                # Calculate hash of converted file
                file_shasum = calculate_shasum(output_path)
                
                # Prepare metadata entry
                entry = {
                    'file_name': f"data/{safe_id}.wav",
                    'shasum': file_shasum,
                    'original_filename': original_title,
                    'transcription': ''  # Placeholder for transcription
                }
                
                # Remove any existing entry with same filename
                metadata = [
                    item for item in metadata 
                    if item.get('file_name') != f"data/{safe_id}.wav"
                ]
                
                # Add new entry
                metadata.append(entry)
                
                # remove original file
                os.remove(input_path)

                # Write updated metadata
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    for item in metadata:
                        f.write(json.dumps(item) + '\n')
                
                logger.info(f"Processed: {input_path}")
            else:
                logger.warning(f"Failed to convert: {input_path}")
        
        except Exception as e:
            logger.error(f"Error processing {input_path}: {str(e)}")
    
    logger.info("Audio processing complete")

# Usage example
if __name__ == "__main__":
    raw_audio_dirs = ['tmp/downloaded/crawled-audio/audio', 'tmp/downloaded/raw-audio/audio']
    combined_raw_dirs = merge_directory_paths(raw_audio_dirs)  
    root_dataset_dir = 'tmp/commercial_dataset/data'
    metadata_path = 'tmp/commercial_dataset/metadata.jsonl'

    process_audio_files(combined_raw_dirs, root_dataset_dir)
