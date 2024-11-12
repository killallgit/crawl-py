import os
import json
import whisper
import multiprocessing
from tqdm import tqdm
from typing import List, Dict, Optional, Callable, Any
import logging
import functools
import torch
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_transcriber(model_size: str = 'medium') -> Callable[[str], Dict[str, str]]:
    """
    Create and return a Whisper transcription function
    
    :param model_size: Size of the Whisper model
    :return: Transcription function
    """
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logger.info(f"Loading Whisper {model_size} model")
    model = whisper.load_model(model_size, device=device, in_memory=True)
    return model.transcribe # type: ignore

def transcribe_single_file(
    transcribe_func: Callable[[str], Dict[str, str]], 
    file_path: str
) -> Optional[str]:
    """
    Transcribe a single audio file
    
    :param transcribe_func: Transcription function
    :param file_path: Path to audio file
    :return: Transcription text or None
    """
    try:
        # Validate file existence
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return None
        
        # Transcribe
        logger.info(f"Transcribing {file_path}")
        result = transcribe_func(file_path, fp16=False) # type: ignore
        logger.info("Transcription complete")
        return result['text'].strip()
    
    except Exception as e:
        logger.error(f"Transcription error for {file_path}: {e}")
        return None

def parallel_transcribe(
    transcribe_func: Callable[[str], Dict[str, str]],
    file_paths: List[str],
    num_workers: Optional[int] = None
) -> List[Optional[str]]:
    """
    Parallel transcription of multiple files
    
    :param transcribe_func: Transcription function
    :param file_paths: List of file paths to transcribe
    :param num_workers: Number of parallel workers
    :return: List of transcriptions
    """
    # Determine number of workers
    workers = num_workers or max(1, multiprocessing.cpu_count() - 2)
    
    # Create a partial function with the transcriber
    transcribe_partial = functools.partial(transcribe_single_file, transcribe_func)
    
    # Use multiprocessing for parallel transcription
    with multiprocessing.Pool(workers) as pool:
        transcriptions = list(tqdm(
            pool.imap(transcribe_partial, file_paths), 
            total=len(file_paths)
        ))
    
    return transcriptions

def load_metadata(
    metadata_path: str
) -> List[Dict[str, Any]]:
    """
    Load metadata from JSONL file
    
    :param metadata_path: Path to metadata file
    :return: List of metadata
    """
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f]
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
        raise

def save_metadata(
    metadata: List[Dict[str, Any]], 
    metadata_path: str
) -> None:
    """
    Save metadata to JSONL file
    
    :param metadata: Metadata to save
    :param metadata_path: Path to save metadata
    """
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            for entry in metadata:
                f.write(json.dumps(entry) + '\n')
        logger.info(f"Metadata saved to {metadata_path}")
    except Exception as e:
        logger.error(f"Error saving metadata: {e}")
        raise

def process_transcription(
    metadata: List[Dict[str, Any]],
    dataset_dir: str,
    transcribe_func: Callable[[str], Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Process transcription for files without existing transcription
    
    :param metadata: Original metadata
    :param dataset_dir: Root dataset dir
    :param transcribe_func: Transcription function
    :return: Updated metadata
    """
    # Filter files to transcribe
    to_transcribe = [
        entry for entry in metadata 
        if not entry.get('transcription')
    ]
    logger.info(f"Found {len(to_transcribe)} files to transcribe")
    
    # Prepare full file paths
    full_paths = [
        os.path.join(dataset_dir, entry['file_name']) 
        for entry in to_transcribe
    ]
    
    # Perform transcription
    transcriptions = parallel_transcribe(transcribe_func, full_paths)
    
    # Update metadata
    for entry, transcription in zip(to_transcribe, transcriptions):
        if transcription:
            entry['transcription'] = transcription
    
    return metadata

def transcribe_dataset(
    metadata_path: str = 'commercial_dataset/metadata.jsonl',
    dataset_dir: str = 'commercial_dataset',
    model_size: str = 'medium'
) -> None:
    """
    Main orchestration function for dataset transcription
    
    :param metadata_path: Path to metadata file
    :param dataset_dir: Root dataset dir
    :param model_size: Whisper model size
    """
    try:
        # Create transcription function
        transcribe_func = create_transcriber(model_size)
        
        # Load metadata
        metadata = load_metadata(metadata_path)
        
        # Process transcription
        updated_metadata = process_transcription(
            metadata, 
            dataset_dir, 
            transcribe_func
        )
        
        # Save updated metadata
        save_metadata(updated_metadata, metadata_path)
        
        logger.info("Transcription process completed successfully")
    
    except Exception as e:
        logger.error(f"Transcription failed: {e}")
        raise

# Execution entry points
def main():
    transcribe_dataset()

if __name__ == '__main__':
    main()
