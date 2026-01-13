import psycopg2

from abc import ABC

from typing import Any


# class DBClass(ABC):
#     def __init__(self, db_name, params):
#         self.db_name = db_name
#         self.params = params
#         self.conn = psycopg2.connect(db_name, params)
#
#     def close_conn(self, conn):
#         self.conn.close()

class DBManager():
    # def __init__(self, db_name, params):
    #     super().__init__(db_name, params)
    #     self.conn.autocommit = True
    #
    #
    # def close_conn(self, conn):
    #     super().__init__(conn)
    def __init__(self, db_name, params):
        self.db_name = db_name
        self.params = params
        self.conn = psycopg2.connect(dbname=db_name, **params)

    def close_conn(self, conn):
        self.conn.close()

    def get_companies_and_vacancies_count(self) -> list[dict[str, Any]]:
        """ — получает список всех компаний и количество вакансий у каждой компании."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute("""
                select employer_name, count_vac  from employers 
                left join (SELECT COUNT(vacansy_id) AS count_vac, employer_id
                    FROM vacansies
                    GROUP BY employer_id
                    ) as  vacansies Using(employer_id);
            """)
            # data_employer.append(cur.fetchall()[0])RETURNING employer_name, count_vac
            # data_count.append(cur.fetchall()[1])
            # data_employers.append({
            #     'employer_name': cur.fetchall()[0],
            #     'count_vacansies': cur.fetchall()[1]
            # })
            data_employers = cur.fetchall()
        return data_employers


    def get_all_vacancies(self) -> list[dict[str, Any]]:
        """получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию."""
        data_employers = []

        with self.conn.cursor() as cur:
            cur.execute("""
                select employer_name, vacansy_name,  salary_from||' - '||salary_to||' '||currency salary, url  from employers 
                left join vacansies on Using(employer_id)  
            """)
            data_employers.append({
                'employer_name': cur.fetchall()[0],
                'vacansy_name': cur.fetchall()[1],
                'salary': cur.fetchall()[2],
                'url': cur.fetchall()[3],
            })
        return data_employers


    def get_avg_salary(self) -> int:
        """ — получает среднюю зарплату по вакансиям."""
        salary_avg = 0
        with self.conn.cursor() as cur:
            cur.execute("""
                        select avg(salary_from) avg_salary_from, avg(salary_to) avg_salary_to from vacansies
                    """)
            salary_avg = cur.fetchall()[0]
        return salary_avg


    def get_vacancies_with_higher_salary(self) -> list[dict[str, Any]]:
        """— получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        salary_avg = get_avg_salary()
        with self.conn.cursor() as cur:
            cur.execute(f"select vacansy_name  from  vacansies where salary_from > {salary_avg}")
            data_employers = cur.fetchall()[0]
        return data_employers


    def get_vacancies_with_keyword(self, list_words: list) -> list[dict[str, Any]]:
        """— получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        data = []
        with self.conn.cursor() as cur:
            cur.execute(f"select vacansy_name  from  vacansies where vacansy_name like in {list_words}")
            data = cur.fetchall()[0]
            return data
