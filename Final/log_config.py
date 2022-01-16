def start_log(logging_file):
    import os
    from datetime import datetime
    import logging
    import re

    now = datetime.now()
    log_time = now.strftime("%m_%d_%y_%H_%M_%S") 

    logger= logging.getLogger(__name__)
    file_handler = logging.FileHandler(logging_file)
    logger.addHandler(file_handler)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s:[%(name)s]: %(message)s')
    file_handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    return logger

if __name__ == "__main__":
    start_log()