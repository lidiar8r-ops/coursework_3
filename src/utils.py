from typing import Any, Dict, List, Union

import psycopg2
import requests

from src import app_logger
from src.config import area_hh

# Настройка логирования
logger = app_logger.get_logger("utils.log")


def get_hh_data(employer_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Получение данных о компаниях и вакансиях с помощью API hh.ru.

    Args:
        employer_ids: Список ID работодателей.

    Returns:
        Список словарей с данными о работодателях и их вакансиях.
    """
    timeout = 10
    max_pages = 20  # 20 * 100 = 2000 вакансий максимум

    data: List[Dict[str, Any]] = []

    for employer_id in employer_ids:
        try:
            # Получаем данные о работодателе
            employer_url = f"https://api.hh.ru/employers/{employer_id}"
            response = requests.get(employer_url, timeout=timeout)

            if response.status_code == 404:
                logger.warning(f"Работодатель {employer_id} не найден")
                continue
            elif response.status_code != 200:
                logger.error(
                    f"Ошибка API при запросе работодателя {employer_id}: "
                    f"статус {response.status_code}, ответ {response.text}"
                )
                continue

            employer_data = response.json()
            if not isinstance(employer_data, dict):
                logger.error(f"Ответ API для {employer_id} не является словарём")
                continue

            # Получаем вакансии работодателя
            all_vacancies: List[Dict[str, Any]] = []
            page = 0

            while page < max_pages:
                vacancies_url = "https://api.hh.ru/vacancies"

                params: Dict[str, Union[str, int, None]] = {
                    "employer_id": employer_id,
                    "per_page": 100,
                    "area": str(area_hh),  # Приводим к str, если area_hh — число
                    "page": page,
                }

                response = requests.get(vacancies_url, params=params, timeout=timeout)

                if response.status_code == 429:
                    logger.error("Превышен лимит запросов к API. Попробуйте позже.")
                    break
                elif response.status_code != 200:
                    logger.error(
                        f"Ошибка API при запросе вакансий {employer_id}, страница {page}: "
                        f"статус {response.status_code}, ответ {response.text}"
                    )
                    break

                vacancies_data = response.json()

                # Проверяем наличие 'items'
                if "items" not in vacancies_data:
                    logger.error(f"В ответе API для {employer_id} нет ключа 'items'")
                    break

                items = vacancies_data["items"]
                if not items:  # Нет больше вакансий
                    break

                all_vacancies.extend(items)
                page += 1

            data.append({"employer": employer_data, "vacancies": all_vacancies})

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при запросе {employer_id}: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка для {employer_id}: {e}")

    logger.info("Получение данных о компаниях и вакансиях завершено")
    return data


def create_database(database_name: str, params: dict) -> None:
    """Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях."""

    conn = psycopg2.connect(dbname="postgres", **params)
    if conn is None:
        logger.error("Соединение с БД не установлено")
        raise ValueError("Соединение с БД не установлено")

    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
        cur.execute(f"CREATE DATABASE {database_name}")
    except psycopg2.errors.ObjectInUse:
        cur.execute(
            f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity WHERE datname = '{database_name}' AND pid <> pg_backend_pid();
        """
        )
        cur.execute(f"DROP DATABASE {database_name}")
        cur.execute(f"CREATE DATABASE {database_name}")
    except psycopg2.errors.InvalidCatalogName:
        cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE employers  (
                    employer_id SERIAL PRIMARY KEY,
                    employer_name VARCHAR(255),
                    site_url TEXT,
                    vacancies_url TEXT,
                    description text,
                    area_name Varchar(255)
                )
            """
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE vacancies (
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
            """
            )

    except Exception as e:
        print(e)
        logger.error(e)

    conn.commit()
    conn.close()
    logger.info("Успешное создание базы данных и таблиц для сохранения данных о компаниях и вакансиях")


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """Сохранение данных о компаниях и вакансиях в базу данных."""
    conn = psycopg2.connect(dbname=database_name, **params)
    try:
        with conn.cursor() as cur:
            # Проверка существования таблицы
            try:
                cur.execute("SELECT 1 FROM employers LIMIT 1")
            except Exception as e:
                logger.error(f"Ошибка проверки таблицы employers: {e}")
                print(f"Ошибка проверки таблицы employers: {e}")
                conn.rollback()
                return  # Прекращаем выполнение при ошибке

            for employer in data:
                employer_data = employer["employer"]

                # Вставка работодателя
                cur.execute(
                    """
                    INSERT INTO employers (employer_name, site_url, vacancies_url, description, area_name)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING employer_id
                    """,
                    (
                        employer_data["name"],
                        employer_data["site_url"],
                        employer_data["vacancies_url"],
                        employer_data["description"],
                        employer_data["area"]["name"],
                    ),
                )
                employer_id = cur.fetchone()[0]

                # Обработка вакансий
                vacancies_data = employer["vacancies"]
                for vacancy_data in vacancies_data:
                    # 1. Обработка зарплаты
                    salary = vacancy_data.get("salary")
                    salary_from = 0.0
                    salary_to = 0.0
                    currency = "RUR"

                    if isinstance(salary, dict):
                        salary_from = salary.get("from", 0.0)
                        salary_to = salary.get("to", 0.0)
                        currency = salary.get("currency", "RUR")

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
                    published_at = vacancy_data.get("published_at")
                    if isinstance(published_at, dict):
                        published_at = published_at.get("$date")
                    elif not isinstance(published_at, str):
                        published_at = None

                    # Вставка вакансии
                    cur.execute(
                        """
                        INSERT INTO vacancies (
                            employer_id, vacansy_name, url, salary_from, salary_to, salary_avg, currency, published_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            employer_id,
                            vacancy_data["name"],
                            vacancy_data["url"],
                            salary_from,
                            salary_to,
                            salary_avg,
                            currency,
                            published_at,
                        ),
                    )

        # Успешное завершение — коммит транзакции
        conn.commit()
        print("Данные успешно сохранены в БД.")
        logger.info("Данные успешно сохранены в БД.")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"Критическая ошибка: {e}")
        conn.rollback()  # Откат при любой ошибке
    finally:
        conn.close()
        logger.info("Окончание сохранения данных о компаниях и вакансиях в базу данных ")
