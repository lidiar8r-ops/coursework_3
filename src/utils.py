from typing import Any
import psycopg2
import requests

from src import app_logger

# Настройка логирования
logger = app_logger.get_logger("utils.log")

def get_hh_data(employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о компаниях и вакансиях с помощью API """

    data = []
    for employer_id in employer_ids:
        url = f"https://api.hh.ru/employers/{employer_id}"

        try:
            vacansies_data = []
            # while True:
            response = requests.get(url)

            if response.status_code == 200:
                employer_data = response.json()
                if not isinstance(employer_data, dict):
                    logger.error("Ответ API не является словарём")
                    return None

            response_vacancy = requests.get(employer_data['vacancies_url'])
            if response_vacancy.status_code == 200:
                vacansies_data = response_vacancy.json()
                # if not isinstance(vacansies_data, dict):
                #     logger.info(f"Отсутствуют вакансии у работадателя {employer_data['name']} с id = {employer_id}")

            data.append({
                'employer': employer_data, #['items'][0],
                'vacansies': vacansies_data['items']
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

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

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
                currency VARCHAR(5),           
                published_at DATE                
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохранение данных о компаниях и вакансиях в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in data:
            employer_data = employer['employer']
            cur.execute(
                """
                INSERT INTO employers (employer_name, site_url, vacancies_url, description, area_name)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING employer_id
                """,
                (employer_data['name'], employer_data['site_url'], employer_data['vacancies_url'],
                employer_data['description'],  employer_data['area']['name'])
            )
            employer_id = cur.fetchone()[0]
            vacansies_data = employer['vacansies']
            for vacancy_data in vacansies_data:
                # 1. Извлекаем данные из salary
                salary = vacancy_data.get('salary')
                if isinstance(salary, dict):
                    salary_from = salary.get('from', 0)
                    salary_to = salary.get('to', 0)
                    currency = salary.get('currency', ' ')
                else:
                    salary_from = 0
                    salary_to = 0
                    currency = 'RUR'

                # 2. Обрабатываем published_at (может быть dict или str)
                published_at = vacancy_data.get('published_at')
                if isinstance(published_at, dict):
                    published_at = published_at.get('$date')  # если есть ключ $date
                elif not isinstance(published_at, str):
                    published_at = None  # если не строка и не dict — ставим None

                cur.execute(
                    """
                    INSERT INTO vacansies (employer_id, vacansy_name, url, salary_from, salary_to, currency, 
                    published_at) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (employer_id,
                    vacancy_data['name'],
                    vacancy_data['url'],
                    salary_from,
                    salary_to,
                    currency,
                    published_at)
                )


    conn.commit()
    conn.close()