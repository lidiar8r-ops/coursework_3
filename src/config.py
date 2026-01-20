# Настройки БД
import os
from configparser import ConfigParser
from typing import Any, Dict

# Пути к файлам
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
LOG_DIR = os.path.join(PARENT_DIR, "logs")
DATA_DIR = os.path.join(PARENT_DIR, "data")
URL_HH = "https://api.hh.ru/"

filename_areas = os.path.join(DATA_DIR, "areas.json")
filename_vacan = os.path.join(DATA_DIR, "vacancies.json")

area_hh = 104  # Челябинск

def config(
    filename: str = "database.ini",
    section: str = "postgresql"
) -> Dict[str, Any]:
    """
    Читает параметры подключения к БД из INI‑файла.

    Args:
        filename: Путь к INI‑файлу (по умолчанию 'database.ini').
        section: Секция с параметрами БД (по умолчанию 'postgresql').

    Returns:
        Словарь с параметрами подключения (например, host, database, user, password).

    Raises:
        FileNotFoundError: Если файл не найден.
        ValueError: Если секция не найдена в файле.
    """
    # Создаём парсер
    parser = ConfigParser()

    # Пытаемся прочитать файл
    if not parser.read(filename):
        raise FileNotFoundError(f"Файл конфигурации не найден: {filename}")

    if parser.has_section(section):
        params = parser.items(section)
        return {param[0]: param[1] for param in params}
    else:
        raise ValueError(f"Секция '{section}' не найдена в файле '{filename}'")
