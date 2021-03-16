import os
from pathlib import Path
import logging
import configparser


PARENT_PATH = os.fspath(Path(__file__).parents[0])
LOGGING_FILE_PATH = os.path.join(PARENT_PATH, "__logger", "{}.log")


def config_reader(file_path, section):
    config = configparser.ConfigParser()
    config.read(file_path)
    return dict(config.items(section))



def set_logger(file_path_extension):
    '''A logging helper.
    Keeps the logged experiments in the __logger path.
    Both prints out on the Terminal and writes on the
    .log file.'''
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)-7s: %(levelname)-1s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(
                LOGGING_FILE_PATH.format(file_path_extension)
            ),
            logging.StreamHandler()
        ])
    return logging


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {msg}: {err}")
    else:
        print("Message produced for Kafka message object...")