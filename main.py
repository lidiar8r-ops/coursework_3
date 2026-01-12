import os
from src.utils import get_hh_data, create_database, save_data_to_database
from config import config
from src import app_logger
from src.api_hh import HeadHunterAPI


# Настройка логирования
logger = app_logger.get_logger("main.log")


def main():
    api_key = HeadHunterAPI()
    company_id = [
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

    data = get_hh_data(api_key, channel_ids)
    create_database('hh', params)
    save_data_to_database(data, 'hh', params)


if __name__ == '__main__':
    main()