import os

from src.db_manager import DBManager
from src.utils import get_hh_data, create_database, save_data_to_database
from src.config import config
from src import app_logger


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

    # data = get_hh_data(employer_ids)
    # # print(data)
    # create_database('hh_ru', params)
    # save_data_to_database(data, 'hh_ru', params)
    # print('запись в базу окончена')

    db_manager = DBManager('hh_ru', params)

    print("\n", "=" * 30)
    print("Список всех компаний c количеством вакансий:" )
    rows = db_manager.get_companies_and_vacancies_count()
    i = 0
    for row in rows:
        i += 1
        print(f"{i}. '{row[0]}' вакансий {row[1]}")

    print("\n", "=" * 30)
    print("Список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию:")
    rows =  db_manager.get_all_vacancies()
    i = 0
    for row in rows:
        i += 1
        print(f"{i}. '{row[0]}' вакансия {row[1]} зарплаты {row[2]} ссылка на вакансию {row[3]}")

    print("\n", "=" * 30)
    print("Список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию:")
    rows =  db_manager.get_all_vacancies()
    i = 0
    for row in rows:
        i += 1
        print(f"{i}. '{row[0]}' вакансия {row[1]} зарплаты {row[2]} ссылка на вакансию {row[3]}")



    db_manager.close_conn()
if __name__ == '__main__':
    main()