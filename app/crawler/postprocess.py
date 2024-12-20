import logging
def ffmpeg_postprocess(info):
    logging.info("Postprocessing audio")
    return [
        '-ac', '1', 
        '-ar', '16000', 
        '-acodec', 'pcm_s16le', 
    ]