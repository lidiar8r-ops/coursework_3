from abc import ABC
from typing import Any

import psycopg2

from src import app_logger

# Настройка логирования
logger = app_logger.get_logger("db_manager.log")


class DBClass(ABC):
    def __init__(self, db_name, params):
        self.db_name = db_name
        self.params = params

    def close_conn(self):
        self.conn.close()


class DBManager(DBClass):
    def __init__(self, db_name, params):
        super().__init__(db_name, params)
        self.salary_avg = 0

        # Проверяем существование БД перед подключением
        if not self._database_exists(db_name, params):
            #
            logger.error(f"БД '{db_name}' не существует или недоступна!")
            print(f"БД '{db_name}' не существует или недоступна!")
            raise ValueError(f"БД '{db_name}' не существует или недоступна!")

    def _database_exists(self, db_name, params):
        """Проверяет, существует ли БД в кластере PostgreSQL."""
        try:
            # Подключаемся к БД
            self.conn = psycopg2.connect(dbname=db_name, **params)
            self.conn.autocommit = True
            cur = self.conn.cursor()

            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
            exists = cur.fetchone() is not None

            cur.close()
            # conn.close()
            return exists

        except Exception as e:
            logger.error(f"Ошибка при проверке существования БД: {e}")
            return False

    def close_conn(self):
        super().close_conn()

    @staticmethod
    def print_vacancies(vacancies: list[dict]) -> None:
        """
        Выводит информацию о вакансиях в удобной форме.
        Args:
            vacancies : Список вакансий для вывода.
        """

        logger.info("Вывод в консоль информации о вакансиях")

        for i, row in enumerate(vacancies, 1):
            print(f"{i}. '{row[0]}': {row[1]}, зарплата: {row[2]}, ссылка: {row[3]}")

        print("-" * 50)

    def get_companies_and_vacancies_count(self) -> list[dict[str, Any]]:
        """получает список всех компаний и количество вакансий у каждой компании."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select employer_name, count_vac  from employers
                left join (SELECT COUNT(vacansy_id) AS count_vac, employer_id
                    FROM vacansies
                    GROUP BY employer_id
                    ) as  vacansies Using(employer_id);
            """
            )
            data_employers = cur.fetchall()
        return data_employers

    def get_all_vacancies(self) -> list[dict[str, Any]]:
        """получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и
        сылки на вакансию."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url
                from employers
                left join vacansies Using(employer_id)
            """
            )
            data_employers = cur.fetchall()
        return data_employers

    def get_avg_salary(self) -> int:
        """получает среднюю зарплату по вакансиям."""
        self.salary_avg = 0
        with self.conn.cursor() as cur:
            cur.execute(
                """
                       SELECT
                            round(AVG(salary_avg::numeric),2) AS salary_avg
                        FROM vacansies;
                    """
            )
            self.salary_avg = cur.fetchall()[0]

        return self.salary_avg

    def get_vacancies_with_higher_salary(self) -> list[dict[str, Any]]:
        """получает cписок всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        self.salary_avg = self.get_avg_salary()[0]

        with self.conn.cursor() as cur:
            cur.execute(
                f"select employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url "
                f" from vacansies "
                f" left join employers Using(employer_id) "
                f" where salary_avg > {self.salary_avg}"
            )
            data_employers = cur.fetchall()
        return data_employers

    def get_vacancies_with_keyword(self, list_words: list) -> list[dict[str, Any]]:
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        Формирует SQL-условие WHERE для поиска по ключевым словам.
        Поддерживает:
          - список слов: ['python', 'программист']
          - строку с запятыми: 'python, программист'
        Возвращает строку SQL-условия или пустую строку, если нет слов.
        """  # 1. Проверяем тип входных данных и преобразуем в список
        if isinstance(list_words, str):
            # Если строка — разбиваем по запятым и убираем пробелы
            data = [word.strip() for word in list_words.split(",") if word.strip()]
        elif isinstance(list_words, (list, tuple)):
            # Если список/кортеж — берём как есть (убираем пустые строки)
            data = [word for word in list_words if word and str(word).strip()]
        else:
            # Неподдерживаемый тип
            return ""

        # 2. Если слов нет — возвращаем пустую строку
        if not data:
            return ""

        # 3. Формируем условие WHERE
        select_words = " WHERE "
        conditions = []

        for word in data:
            # Экранируем спецсимволы в LIKE (% и _) если нужно (опционально)
            # Здесь пропускаем, т.к. ищем по шаблону
            condition = f"UPPER(vacansy_name ) LIKE UPPER('%{word}%')"
            conditions.append(condition)

        # Объединяем условия через OR
        select_words += " OR ".join(conditions)

        select_words = (
            f"SELECT employer_name, vacansy_name ,  salary_from||' - '||salary_to||' '||currency salary, url "
            f" FROM vacansies "
            f" LEFT JOIN employers Using(employer_id)  {select_words}"
        )
        print(select_words)

        with self.conn.cursor() as cur:
            cur.execute(select_words)
            data = cur.fetchall()

        return data
