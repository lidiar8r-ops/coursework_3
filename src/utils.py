from typing import Any
import psycopg2
import requests

from src import app_logger
from src.config import area_hh

# Настройка логирования
logger = app_logger.get_logger("utils.log")

def get_hh_data(employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о компаниях и вакансиях с помощью API """

    data = []
    for employer_id in employer_ids:
        url = f"https://api.hh.ru/employers/{employer_id}"

        try:
            response = requests.get(url)

            if response.status_code == 200:
                employer_data = response.json()
                if not isinstance(employer_data, dict):
                    logger.error("Ответ API не является словарём")
                    break
            vacansies_data = []

            # print(employer_data)
            # https://api.hh.ru/employers/{employer_id}/vacancies/active
            # "vacancies_url": "https://api.hh.ru/vacancies?employer_id=32575"
            # response_vacancy = requests.get(f"{employer_data['vacancies_url']}&)
            # if response_vacancy.status_code == 200:
            #     vacansies_data = response_vacancy.json()
            #     # if not isinstance(vacansies_data, dict):
            #     #     logger.info(f"Отсутствуют вакансии у работадателя {employer_data['name']} с id = {employer_id}")


            params = {
                "employer_id": employer_id,
                "per_page": 100,  # Макс. 100 на страницу
                "area":  area_hh,
                "page": 0
            }

            all_vacancies = []
            while True:
                # url = f"https://api.hh.ru/vacancies?per_page=100&employer_id={employer_id}&page={params['page']}"
                url = f"https://api.hh.ru/vacancies"

                response = requests.get(url, params=params)  # , params=params
                if response.status_code == 200:
                    vacansies_data = response.json()
                    # print(url,params)
                    # print(vacansies_data)

                if not vacansies_data["items"]:
                    break

                all_vacancies.extend(vacansies_data["items"])
                params["page"] += 1


                # Ограничение API: не более 2000 вакансий
                if params["page"] > 19:
                    break


            # print(all_vacancies)
            data.append({
                'employer': employer_data, #['items'][0],
                'vacansies': all_vacancies  #['items']
            })


        except Exception as e:
            logger.error(e)
    return data


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')
    except psycopg2.errors.ObjectInUse:
        cur.execute(f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity WHERE datname = '{database_name}' AND pid <> pg_backend_pid();
        """)
        cur.execute(f'DROP DATABASE {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')
    except psycopg2.errors.InvalidCatalogName:
        cur.execute(f'CREATE DATABASE {database_name}')

    # cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers  (
                employer_id SERIAL PRIMARY KEY,
                employer_name VARCHAR(255),
                site_url TEXT,
                vacancies_url TEXT,
                description text,
                area_name Varchar(255)                                
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacansies (
                vacansy_id SERIAL PRIMARY KEY,
                employer_id INT REFERENCES employers (employer_id),
                vacansy_name VARCHAR(255) NOT NULL,
                url TEXT, 
                salary_from VARCHAR(30),
                salary_to VARCHAR(30),
                salary_avg REAL,
                currency VARCHAR(5),           
                published_at DATE                
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохранение данных о компаниях и вакансиях в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)

    try:
        with conn.cursor() as cur:
            # Проверка существования таблицы (опционально)
            try:
                cur.execute("SELECT 1 FROM employers LIMIT 1")
            except Exception as e:
                print(f"Ошибка проверки таблицы employers: {e}")
                conn.rollback()
                return  # Прекращаем выполнение при ошибке

            for employer in data:
                employer_data = employer['employer']

                # Вставка работодателя
                cur.execute(
                    """
                    INSERT INTO employers (employer_name, site_url, vacancies_url, description, area_name)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING employer_id
                    """,
                    (
                        employer_data['name'],
                        employer_data['site_url'],
                        employer_data['vacancies_url'],
                        employer_data['description'],
                        employer_data['area']['name']
                    )
                )
                employer_id = cur.fetchone()[0]

                # Обработка вакансий
                vacancies_data = employer['vacansies']
                for vacancy_data in vacancies_data:
                    # 1. Обработка зарплаты
                    salary = vacancy_data.get('salary')
                    salary_from = 0
                    salary_to = 0
                    currency = 'RUR'

                    if isinstance(salary, dict):
                        salary_from = salary.get('from')
                        salary_to = salary.get('to')
                        currency = salary.get('currency', 'RUR')

                        # Преобразование в float с обработкой ошибок
                        try:
                            salary_from = float(salary_from) if salary_from is not None else 0
                        except (TypeError, ValueError):
                            salary_from = 0

                        try:
                            salary_to = float(salary_to) if salary_to is not None else 0
                        except (TypeError, ValueError):
                            salary_to = 0

                    # Расчёт средней зарплаты
                    if salary_from > 0 and salary_to > 0:
                        salary_avg = (salary_from + salary_to) / 2
                    elif salary_from > 0:
                        salary_avg = salary_from
                    elif salary_to > 0:
                        salary_avg = salary_to
                    else:
                        salary_avg = 0

                    # 2. Обработка даты публикации
                    published_at = vacancy_data.get('published_at')
                    if isinstance(published_at, dict):
                        published_at = published_at.get('$date')
                    elif not isinstance(published_at, str):
                        published_at = None

                    # Вставка вакансии
                    cur.execute(
                        """
                        INSERT INTO vacansies (
                            employer_id, vacansy_name, url, salary_from, 
                            salary_to, salary_avg, currency, published_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            employer_id,
                            vacancy_data['name'],
                            vacancy_data['url'],
                            salary_from,
                            salary_to,
                            salary_avg,
                            currency,
                            published_at
                        )
                    )

        # Успешное завершение — коммит транзакции
        conn.commit()
        print("Данные успешно сохранены в БД.")

    except Exception as e:
        print(f"Критическая ошибка: {e}")
        conn.rollback()  # Откат при любой ошибке
    finally:
        conn.close()


