import psycopg2

from abc import ABC

from typing import Any


class DBClass(ABC):
    def __init__(self, db_name, params):
        self.db_name = db_name
        self.params = params
        self.conn = psycopg2.connect(dbname=db_name, **params)
        self.conn.autocommit = True


    def close_conn(self):
        self.conn.close()


class DBManager(DBClass):
    def __init__(self, db_name, params):
        super().__init__(db_name, params)
        self.salary_avg = 0

    def close_conn(self):
        super().close_conn()


    def get_companies_and_vacancies_count(self) -> list[dict[str, Any]]:
        """ получает список всех компаний и количество вакансий у каждой компании."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute("""
                select employer_name, count_vac  from employers 
                left join (SELECT COUNT(vacansy_id) AS count_vac, employer_id
                    FROM vacansies
                    GROUP BY employer_id
                    ) as  vacansies Using(employer_id);
            """)
            data_employers = cur.fetchall()
            # cur.close()
        return data_employers


    def get_all_vacancies(self) -> list[dict[str, Any]]:
        """получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute("""
                select employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url  
                from employers 
                left join vacansies Using(employer_id)  
            """)
            data_employers = cur.fetchall()
        return data_employers


    def get_avg_salary(self) -> int:
        """ получает среднюю зарплату по вакансиям."""
        self.salary_avg = 0
        with self.conn.cursor() as cur:
            cur.execute("""
                       SELECT
                            round(AVG(salary_avg::numeric),2) AS salary_avg
                        FROM vacansies;
                    """)
            self.salary_avg = cur.fetchall()[0]

        return self.salary_avg


    def get_vacancies_with_higher_salary(self) -> list[dict[str, Any]]:
        """— получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        self.salary_avg = self.get_avg_salary()[0]

        # print(self.salary_avg)
        with self.conn.cursor() as cur:
            cur.execute(f"select employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url "
                        f" from vacansies "
                        f" left join employers Using(employer_id) "
                        f" where salary_avg > {self.salary_avg}")
            data_employers = cur.fetchall()
        return data_employers


    def get_vacancies_with_keyword(self, list_words: list) -> list[dict[str, Any]]:
        """ список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        data = []
        select_words = ""
        if len(list_words):
            select_words = " WHERE "

            for word in list_words:
                if select_words != " WHERE ":
                    select_words += " or "
                select_words += f" UPPER(vacansy_name) like UPPER('%{word}%')"

        select_words = (f"SELECT employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url "
                        f" FROM vacansies "
                        f" LEFT JOIN employers Using(employer_id)  {select_words}")
        # print(select_words)

        with self.conn.cursor() as cur:
            cur.execute(select_words)
            data = cur.fetchall()

        return data
