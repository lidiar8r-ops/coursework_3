import os

from src.utils import get_hh_data, create_database, save_data_to_database
from src.config import config
from src import app_logger
from src.api_hh import HeadHunterAPI


# Настройка логирования
logger = app_logger.get_logger("main.log")


def main():
    # api_key = os.getenv('YT_API_KEY')
    employer_ids = [
        '32575',  # АО Метран, Промышленная группа
        '68587',  # Алабуга, ОЭЗ ППТ
        '700330',  # ООО НПО Технодар
        '9870126',  # ООО Челябинский Завод Нестандартного Оборудования
        '2969784',  # Digital Partners Global
        '560984',  # ООО Чебаркульская птица
        '6163006',  # Академия Компьютерных Технологий и Дизайна
        '67788',  # Челябинский кузнечно-прессовый завод (ЧКПЗ)
        '11918231',  #ООО СИТР
        '1035394',  # Красное & Белое, розничная сеть
    ]
    params = config()

    data = get_hh_data(employer_ids)
    # print(data)
    create_database('hh_ru', params)
    save_data_to_database(data, 'hh_ru', params)
    print('запись в базу окончена')

if __name__ == '__main__':
    main()