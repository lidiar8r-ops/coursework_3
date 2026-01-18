# Настройки БД
import os
from configparser import ConfigParser


# Пути к файлам
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
LOG_DIR = os.path.join(PARENT_DIR, "logs")
DATA_DIR = os.path.join(PARENT_DIR, "data")
URL_HH = "https://api.hh.ru/"

filename_areas = os.path.join(DATA_DIR, "areas.json")
filename_vacan = os.path.join(DATA_DIR, "vacancies.json")

area_hh = 104  # Челябинск


def config(filename="database.ini", section="postgresql"):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db
