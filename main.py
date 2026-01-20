from src import app_logger
from src.config import config
from src.db_manager import DBManager
from src.utils import create_database, get_hh_data, save_data_to_database

# Настройка логирования
logger = app_logger.get_logger("main.log")


def main() -> None:
    logger.info("Начало работы программы")
    print("Добро пожаловать в систему работы с вакансиями!")
    print("=" * 50)
    logger.info("=" * 50)

    while True:
        print("\nВыберите действие:")
        print("1. Создание БД и таблицы для хранения данных о работодателях и вакансиях")
        print("2. Список всех компаний и количество вакансий у каждой")
        print("3. Список всех вакансий")
        print("4. Средняя зарплата по всем вакансиям")
        print("5. Список вакансий с зарплатой выше средней")
        print("6. Список вакансий по ключевым словам")
        print("7. Выход")

        choice = input("\nВведите номер действия (1–7): ").strip()

        params = config()

        if choice == "1":
            employer_ids = [
                "32575",  # АО Метран, Промышленная группа
                "68587",  # Алабуга, ОЭЗ ППТ
                "700330",  # ООО НПО Технодар
                "9870126",  # ООО Челябинский Завод Нестандартного Оборудования
                "2969784",  # Digital Partners Global
                "560984",  # ООО Чебаркульская птица
                "6163006",  # Академия Компьютерных Технологий и Дизайна
                "67788",  # Челябинский кузнечно-прессовый завод (ЧКПЗ)
                "11918231",  # ООО СИТР
                "1035394",  # Красное & Белое, розничная сеть
                "2180",  # ОЗОН
            ]

            data = get_hh_data(employer_ids)
            create_database("hh_ru", params)
            save_data_to_database(data, "hh_ru", params)
            print("Запись в базу завершена")
            logger.info("Запись в базу завершена")

        elif choice == "2" or choice == "3" or choice == "4" or choice == "5" or choice == "6":
            try:
                db_manager = DBManager("hh_ru", params)

                if choice == "2":
                    print("\n", "=" * 30)
                    print("Список всех компаний с количеством вакансий:")
                    rows = db_manager.get_companies_and_vacancies_count()
                    for i, row in enumerate(rows, 1):
                        print(f"{i}. '{row[0]}' — {row[1]} вакансий")

                elif choice == "3":
                    print("\n", "=" * 30)
                    print("Список всех вакансий (компания, название, зарплата, ссылка):")
                    rows = db_manager.get_all_vacancies()
                    db_manager.print_vacancies(rows)

                elif choice == "4":
                    print("\n", "=" * 30)
                    print("Средняя зарплата по вакансиям:")
                    avg_salary = db_manager.get_avg_salary()
                    print(f"Средняя зарплата: {avg_salary if avg_salary else 'Нет данных'}")

                elif choice == "5":
                    print("\n", "=" * 30)
                    print("Вакансии с зарплатой выше средней:")
                    rows = db_manager.get_vacancies_with_higher_salary()
                    db_manager.print_vacancies(rows)

                elif choice == "6":
                    print("\n", "=" * 30)
                    print("Вакансии по ключевым словам (например, программист, python, курьер):")
                    excluded_text = input("Введите слова для запроса (через запятую): ").strip()
                    # print(excluded_text)
                    rows = db_manager.get_vacancies_with_keyword(excluded_text)
                    db_manager.print_vacancies(rows)

                db_manager.close_conn()  # Закрываем соединение после всех операций
            except Exception as e:
                print(e)
                logger.error("Сперва создайте БД, выбрав пункт 1")
                print("Сперва создайте БД, выбрав пункт 1")

        elif choice == "7":
            print("Завершение работы...")
            break

        else:
            print("Неверный ввод. Введите число от 1 до 7")


if __name__ == "__main__":
    main()
