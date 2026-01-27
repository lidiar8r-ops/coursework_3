import logging
import os
from logging import Logger

from src.config import LOG_DIR

s_log_format: str = "%(asctime)s - [%(levelname)s] - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"


def get_file_handler(name: str) -> logging.FileHandler:
    """
    Создаёт файловый обработчик логов.

    :param name: имя файла лога (без пути)
    :return: экземпляр FileHandler
    """
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, name), mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(s_log_format))
    return file_handler


def get_stream_handler() -> logging.StreamHandler:
    """
    Создаёт обработчик вывода в консоль (только ошибки).

    :return: экземпляр StreamHandler
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(logging.Formatter(s_log_format))
    return stream_handler


def get_logger(name: str) -> Logger:
    """
    Создаёт логгер с файловым и консольным обработчиками.

    :rtype: Logger
    :param name: имя логгера (обычно __name__ модуля)
    :return: настроеннный экземпляр Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Добавляем обработчики
    logger.addHandler(get_file_handler(name))
    # logger.addHandler(get_stream_handler())

    return logger
