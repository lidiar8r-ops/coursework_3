from typing import Any
import psycopg2
import requests

from src import app_logger

# Настройка логирования
logger = app_logger.get_logger("utils.log")

# https://api.hh.ru/employers/{employer_id}
# def get_hh_data(api_key: str, employer_ids: list[str]) -> list[dict[str, Any]]:
def get_hh_data(employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о компаниях и вакансиях с помощью API """

    data = []
    for employer_id in employer_ids:
        url = f"https://api.hh.ru/employers/{employer_id}"

        try:
            vacanies_data = []
            # while True:
            response = requests.get(url)

            if response.status_code == 200:
                employer_data = response.json()
                if not isinstance(employer_data, dict):
                    logger.error("Ответ API не является словарём")
                    return None
                # print(employer_data)
                # print(employer_data['vacancies_url'])

            response_vacancy = requests.get(employer_data['vacancies_url'])
            if response_vacancy.status_code == 200:
                vacanies_data = response_vacancy.json()
                # if not isinstance(vacanies_data, dict):
                #     logger.info(f"Отсутствуют вакансии у работадателя {employer_data['name']} с id = {employer_id}")
                # print("="*20)
                # print(vacanies_data)
            data.append({
                'employer': employer_data, #['items'][0],
                'vacanies': vacanies_data['items']
            })
        except Exception as e:
            logger.error(e)
    return data


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

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
                employer_data['area']['name'])
            )
            employer_id = cur.fetchone()[0]
            vacanies_data = employer['vacanies']
            for vacancy_data in vacanies_data:
                cur.execute(
                    """
                    INSERT INTO vacanies (employer_id, vacansy_name, url, salary_from, salary_to, currency, 
                    published_at) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (employer_id, vacancy_data['name'], vacancy_data['url'], vacancy_data.get('salary', {'from': 0}),
                     vacancy_data.get('salary', {'to': 0}), vacancy_data.get('salary', {'currency': 0}),
                     vacancy_data['published_at'])
                )


    conn.commit()
    conn.close()