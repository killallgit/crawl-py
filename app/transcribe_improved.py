import os
import json
import whisper
import multiprocessing
from tqdm import tqdm
from typing import List, Dict, Optional, Callable, Any
import logging
import functools
import torch

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def create_transcriber(model_size: str = 'medium') -> Callable[[str], Dict[str, str]]:
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logger.info(f"Loading Whisper {model_size} model")
    model = whisper.load_model(model_size, device=device, in_memory=True)
    return model.transcribe  # type: ignore

def transcribe_and_update_metadata(
    transcribe_func: Callable[[str], Dict[str, str]],
    metadata_path: str,
    file_path: str,
    file_name: str
) -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return None
        
        # Transcribe
        logger.info(f"Transcribing {file_path}")
        result = transcribe_func(file_path, fp16=False)  # type: ignore
        logger.info("Transcription complete")
        text = result['text'].strip()
        
        # Prepare metadata entry
        metadata_entry = {
            "file_name": file_name,
            "text": text,
            "class_labels": ["commercial"]
        }
        
        # Append metadata entry to the file
        with open(metadata_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(metadata_entry) + '\n')
        
        return metadata_entry
    
    except Exception as e:
        logger.error(f"Transcription error for {file_path}: {e}")
        return None

def parallel_transcribe_and_update(
    transcribe_func: Callable[[str], Dict[str, str]],
    metadata_path: str,
    file_paths: List[str],
    file_names: List[str],
    num_workers: Optional[int] = None
) -> List[Optional[Dict[str, Any]]]:
    workers = num_workers or max(1, multiprocessing.cpu_count() - 2)
    
    transcribe_partial = functools.partial(
        transcribe_and_update_metadata,
        transcribe_func,
        metadata_path
    )
    
    with multiprocessing.Pool(workers) as pool:
        args = zip(file_paths, file_names)
        results = list(tqdm(
            pool.starmap(transcribe_partial, args),
            total=len(file_paths)
        ))
    
    return results

def load_metadata(metadata_path: str) -> List[Dict[str, Any]]:
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f]
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
        raise

def save_metadata(metadata: List[Dict[str, Any]], metadata_path: str) -> None:
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
    transcribe_func: Callable[[str], Dict[str, str]],
    metadata_path: str
) -> List[Dict[str, Any]]:
    to_transcribe = [
        entry for entry in metadata
        if not entry.get('text')
    ]
    logger.info(f"Found {len(to_transcribe)} files to transcribe")
    
    full_paths = [
        os.path.join(dataset_dir, entry['file_name'])
        for entry in to_transcribe
    ]
    
    file_names = [entry['file_name'] for entry in to_transcribe]
    
    # Clear the metadata file before starting
    open(metadata_path, 'w').close()
    
    # Transcribe and update metadata in parallel
    updated_entries = parallel_transcribe_and_update(
        transcribe_func,
        metadata_path,
        full_paths,
        file_names
    )
    
    # Update the original metadata list with transcribed entries
    file_name_to_entry = {entry['file_name']: entry for entry in metadata}
    for entry in updated_entries:
        if entry:
            file_name_to_entry[entry['file_name']].update(entry)
    
    return metadata

def transcribe_dataset(
    root_dir: str = '/Volumes/data',
    model_size: str = 'medium'
) -> None:
    try:
        metadata_path = os.path.join(root_dir, 'dataset/metadata.jsonl')
        transcribe_func = create_transcriber(model_size)
        metadata = load_metadata(metadata_path)
        updated_metadata = process_transcription(
            metadata,
            root_dir,
            transcribe_func,
            metadata_path
        )
        
        # Optionally, save the complete metadata again if needed
        # save_metadata(updated_metadata, metadata_path)
        
        logger.info("Transcription process completed successfully")
    
    except Exception as e:
        logger.error(f"Transcription failed: {e}")
        raise

def main():
    transcribe_dataset()

if __name__ == '__main__':
    main()