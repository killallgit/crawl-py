import os

def ensure_directory(directory: str) -> str:
    """Ensure directory exists and return its path."""
    os.makedirs(directory, exist_ok=True)
    return directory


def find_audio_file(directory: str, video_id: str) -> str | None:
    """Find the audio file for a given video ID."""
    audio_files = [
        os.path.join(directory, f) 
        for f in os.listdir(directory) 
        if video_id in f and f.endswith('.wav')
    ]
    return audio_files[0] if audio_files else None
