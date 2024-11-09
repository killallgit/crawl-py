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
    """
    Sanitize filename by removing special characters and replacing with underscores
    """
    # Remove or replace characters that might cause issues in filenames
    sanitized = re.sub(r'[^\w\-_\.]', '_', filename)
    
    # Ensure filename is not empty
    return sanitized if sanitized else "unnamed_file"

def process_audio_files(input_dir, output_data_dir, metadata_path, 
                        supported_extensions=['.wav', '.mp3', '.flac', '.ogg', '.m4a']):
    """
    Process audio files in a directory, with advanced metadata tracking
    
    :param input_dir: Directory containing source audio files
    :param output_data_dir: Directory to save converted WAV files
    :param metadata_path: Path to metadata JSONL file
    :param supported_extensions: List of supported audio file extensions
    """
    # Create output directories if they don't exist
    os.makedirs(output_data_dir, exist_ok=True)
    
    # Load existing metadata
    metadata = []
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = [json.loads(line.strip()) for line in f if line.strip()]
    except Exception as e:
        logger.warning(f"Error reading existing metadata: {str(e)}")
    
    # Find audio files
    audio_files = [
        f for f in os.listdir(input_dir) 
        if os.path.isfile(os.path.join(input_dir, f)) 
        and os.path.splitext(f)[1].lower() in supported_extensions
    ]
    
    # Process files with progress bar
    for audio_file in tqdm(audio_files, desc="Processing Audio Files"):
        try:
            # Input and output paths
            input_path = os.path.join(input_dir, audio_file)
            
            # Generate a unique, safe filename
            file_name, file_ext = os.path.splitext(audio_file)
            safe_id = sanitize_filename(file_name)
            output_path = os.path.join(output_data_dir, f"{safe_id}.wav")
            
            # Check if file already processed with matching hash
            existing_entry = next(
                (entry for entry in metadata 
                 if entry.get('file_name') == f"data/{safe_id}.wav"), 
                None
            )
            
            # Calculate current input file hash
            current_hash = calculate_shasum(input_path)
            if not current_hash:
                logger.warning(f"Skipping {audio_file}: Could not calculate hash")
                continue
            
            # Skip if already processed with same hash
            if existing_entry and existing_entry.get('shasum') == current_hash:
                logger.info(f"Skipping {audio_file}: Already processed")
                continue
            
            # Convert audio
            if convert_audio_to_wav(input_path, output_path):
                # Calculate hash of converted file
                file_shasum = calculate_shasum(output_path)
                
                # Prepare metadata entry
                entry = {
                    'file_name': f"data/{safe_id}.wav",
                    'shasum': file_shasum,
                    'original_filename': audio_file,
                    'transcription': ''  # Placeholder for transcription
                }
                
                # Remove any existing entry with same filename
                metadata = [
                    item for item in metadata 
                    if item.get('file_name') != f"data/{safe_id}.wav"
                ]
                
                # Add new entry
                metadata.append(entry)
                
                # Write updated metadata
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    for item in metadata:
                        f.write(json.dumps(item) + '\n')
                
                logger.info(f"Processed: {audio_file}")
            else:
                logger.warning(f"Failed to convert: {audio_file}")
        
        except Exception as e:
            logger.error(f"Error processing {audio_file}: {str(e)}")
    
    logger.info("Audio processing complete")

# Usage example
if __name__ == "__main__":
    input_dir = 'crawled-audio/audio'
    output_data_dir = 'commercial_dataset/data'
    metadata_path = 'commercial_dataset/metadata.jsonl'

    process_audio_files(input_dir, output_data_dir, metadata_path)
