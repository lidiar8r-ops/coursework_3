from googleapiclient.discovery import build
from typing import Any
import psycopg2

# https://api.hh.ru/employer/{employer_id}
def get_hh_data(api_key: str, employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о компаниях и вакансиях с помощью API Key."""

    hh = build('hh', 'v3', developerKey=api_key)

    data = []
    for employer_id in employer_ids:
        employer_data = hh.employers().list(part='snippet, statistics', id=employer_id).execute()

        vacanies_data = []
        while True:
            response = hh.search().list(part='id,snippet', employerId=employer_id, type='vacancy',
                                             order='date', maxResults=50, pageToken=next_page_token).execute()
            vacanies_data.extend(response['items'])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        data.append({
            'employer': employer_data['items'][0],
            'vacanies': vacanies_data
        })

    return data


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {database_name}")
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
                currency VARCHAR(5)
                description VARCHAR(355),                
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
                 employer_data['description'], {employer['area']['name']})
            )
            employer_id = cur.fetchone()[0]
            vacanies_data = employer['vacanies']
            for vacancy_data in vacanies_data:
                cur.execute(
                    """
                    INSERT INTO vacanies (employer_id, vacansy_name, url, salary_from, salary_to, currency, description, 
                    published_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (employer_id, vacancy_data['name'], vacancy_data['url'], vacancy_data['salary']['from'],
                     vacancy_data['salary']['to'], vacancy_data['salary']['currency'], vacancy_data['description'],
                     vacancy_data['published_at'])
                )


    conn.commit()
    conn.close()