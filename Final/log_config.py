from datetime import datetime
import logging


def start_log(logging_file_loc):
    now = datetime.now()
    log_time = now.strftime("%m_%d_%y_%H_%M_%S")
    logging_file = logging_file_loc + "/" + log_time + ".log"

    logger = logging.getLogger(__name__)
    file_handler = logging.FileHandler(logging_file)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s:[%(name)s]: %(message)s')

    logger.addHandler(file_handler)
    file_handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    return logger


def start_log_to_stream():
    import logging

    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s:[%(name)s]: %(message)s')

    # Stream Handler will Show all the info on the screen
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    return logger