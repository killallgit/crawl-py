import os
import json
import whisper
import multiprocessing
from tqdm import tqdm
from typing import List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transcribe_audio(file_path: str, model: whisper.Whisper) -> Optional[str]:
    """
    Transcribe a single audio file
    
    :param file_path: Path to audio file
    :param model: Whisper model
    :return: Transcription text or None
    """
    try:
        # Validate file existence
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return None
        
        # Simple transcription
        result = model.transcribe(file_path)
        return result['text'].strip() # type: ignore
    
    except Exception as e:
        logger.error(f"Transcription error for {file_path}: {e}")
        return None

def parallel_transcribe(
    model: whisper.Whisper, 
    file_paths: List[str],
    num_workers: Optional[int] = None
) -> List[Optional[str]]:
    """
    Parallel transcription of multiple files
    
    :param model: Whisper model
    :param file_paths: List of file paths to transcribe
    :param num_workers: Number of parallel workers
    :return: List of transcriptions
    """
    # Determine number of workers
    if num_workers is None:
        num_workers = max(1, multiprocessing.cpu_count() - 2)
    
    # Use multiprocessing for parallel transcription
    with multiprocessing.Pool(num_workers) as pool:
        # Use a lambda to pass both arguments to transcribe_audio
        transcribe_func = lambda path: transcribe_audio(path, model)
        
        transcriptions = list(tqdm(
            pool.imap(transcribe_func, file_paths), 
            total=len(file_paths)
        ))
    
    return transcriptions

def process_dataset(
    metadata_path: str = 'commercial_dataset/metadata.jsonl',
    data_dir: str = 'commercial_dataset/data',
    model_size: str = 'medium'
) -> None:
    """
    Main function to transcribe a dataset
    
    :param metadata_path: Path to metadata file
    :param data_dir: Directory with audio files
    :param model_size: Whisper model size
    """
    try:
        # 1. Load Whisper model
        logger.info(f"Loading Whisper {model_size} model")
        model = whisper.load_model(model_size)
        
        # 2. Load metadata
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = [json.loads(line) for line in f]
        
        # 3. Filter files to transcribe
        to_transcribe = [
            entry for entry in metadata 
            if not entry.get('transcription')
        ]
        logger.info(f"Found {len(to_transcribe)} files to transcribe")
        
        # 4. Prepare full file paths
        full_paths = [
            os.path.join(data_dir, entry['file_name']) 
            for entry in to_transcribe
        ]
        
        # 5. Parallel transcription
        transcriptions = parallel_transcribe(model, full_paths)
        
        # 6. Update metadata
        for entry, transcription in zip(to_transcribe, transcriptions):
            if transcription:
                entry['transcription'] = transcription
        
        # 7. Save updated metadata
        with open(metadata_path, 'w', encoding='utf-8') as f:
            for entry in metadata:
                f.write(json.dumps(entry) + '\n')
        
        logger.info("Transcription process completed successfully")
    
    except Exception as e:
        logger.error(f"Transcription failed: {e}")
        raise

# Execution methods
def main():
    process_dataset()

# Ensure this is the primary way of running the script
if __name__ == '__main__':
    main()

# Alternative ways to import and use
def run_transcription(
    metadata_path: str, 
    data_dir: str, 
    model_size: str = 'medium'
):
    """
    External function to trigger transcription
    """
    process_dataset(metadata_path, data_dir, model_size)
