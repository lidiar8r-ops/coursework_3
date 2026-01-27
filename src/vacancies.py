import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# Настройка логирования
logger = logging.getLogger("vacancy")


class Vacancy:
    """
    Представляет собой одну вакансию с основными атрибутами и методами для работы с ними.

    Атрибуты:
        title (str): Название вакансии.
        url (str): Адрес страницы вакансии.
        salary (str): Информация о зарплате (может содержать диапазон, префиксы «от/до»).
        description (str): Краткое описание вакансии.
        employer (str): Название работодателя.
        published_at (str): Дата публикации вакансии.


    Основные возможности:
        - Валидация входных данных (URL, обязательные поля).
        - Извлечение числового значения зарплаты для сортировки.
        - Сериализация в словарь (to_dict).
        - Сравнение вакансий по зарплате (>, <, == и др.).
        - Создание экземпляра из ответа API hh.ru (from_hh_api).
        - Удобное строковое представление (str, repr).
        Методы:
        get_salary_value() -> float: Возвращает численное значение зарплаты для сортировки.
        to_dict() -> Dict[str, Any]: Преобразует объект в словарь для дальнейшей сериализации.
        from_hh_api(item: Dict[str, Any]) -> Vacancy: Создает объект Vacancy из ответа API hh.ru.
        print_vacancies(vacancies: List['Vacancy']): Печать информации о списке вакансий.
    """

    __slots__ = ["_title", "_url", "_salary", "_description", "_employer", "_published_at"]

    def __init__(self, title: str, url: str, salary: str, description: str, employer: str, published_at: str):
        """
        Инициализирует объект Vacancy.


        Args:
            title: Название вакансии.
            url: URL страницы вакансии.
            salary: Информация о зарплате (строка).
            description: Описание вакансии.
            employer: Название работодателя.
            published_at: Дата публикации.


        Raises:
            ValueError: Если URL отсутствует или некорректен.
        """
        # Валидация URL
        if not url or not url.strip():
            logger.error("URL вакансии обязателен")
            raise ValueError("URL вакансии обязателен")
        if not self._is_valid_url(url.strip()):
            logger.error(f"Некорректный URL: {url}")
            raise ValueError(f"Некорректный URL: {url}")

        # Инициализация атрибутов с очисткой строк
        self._title = title.strip()
        self._url = url.strip()
        self._salary = self._process_salary(salary)
        self._description = description.strip() if description and description.strip() else "Описание не указано"
        self._employer = employer.strip() if employer and employer.strip() else "Работодатель не указан"
        self._published_at = (published_at or "").strip()

    def _is_valid_url(self, url: str) -> bool:
        """Проверяет, является ли строка корректным URL."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    def _process_salary(self, salary: Optional[str]) -> str:
        """Приводит строку с зарплатой к стандартному виду."""
        if salary is None or not str(salary).strip():
            return "Зарплата не указана!"
        return str(salary).strip()

    def get_salary_value(self) -> float:
        """
        Извлекает числовое значение зарплаты.

        Для диапазонов возвращает среднее значение. Если числовые данные не найдены — 0.0.

        Returns:
            float: Числовое значение зарплаты или 0.0, если не указано.
        """
        if "Зарплата не указана!" in self._salary:
            return 0.0

        # Ищем числа (с пробелами‑разделителями тысяч)
        numbers = re.findall(r"\b\d{1,3}(?:\s?\d{3})*\b", self._salary.replace("\u202f", ""))
        values = []

        for num_str in numbers:
            cleaned = num_str.replace(" ", "")
            try:
                values.append(float(cleaned))
            except ValueError:
                continue

        if len(values) >= 2:
            return sum(values) / len(values)  # Среднее для диапазона
        if values:
            return values[0]  # Единственное значение
        return 0.0  # Не удалось извлечь числа

    # Геттеры (через @property)
    @property
    def title(self) -> str:
        return self._title

    @property
    def url(self) -> str:
        return self._url

    @property
    def description(self) -> str:
        return self._description

    @property
    def employer(self) -> str:
        return self._employer

    @property
    def published_at(self) -> str:
        return self._published_at

    @property
    def salary(self) -> str:
        return self._salary

    @salary.setter
    def salary(self, value: str) -> None:
        self._salary = self._process_salary(value)

    def to_dict(self) -> Dict[str, Any]:
        """
        Преобразует объект в словарь для сериализации (например, в JSON).

        Returns:
            Dict[str, Any]: Словарь с данными вакансии.
        """
        return {
            "title": self.title,
            "url": self.url,
            "salary": self.salary,
            "description": self.description,
            "employer": self.employer,
            "published_at": self.published_at,
        }

    @classmethod
    def from_hh_api(cls, item: Dict[str, Any]) -> "Vacancy":
        """
        Создаёт экземпляр Vacancy из ответа API hh.ru.

        Args:
            item: Словарь с данными вакансии из API hh.ru.

        Returns:
            Vacancy: Готовый объект Vacancy.
        """
        salary_info = item.get("salary")
        salary_str = ""

        if salary_info:
            salary_from = salary_info.get("from")
            salary_to = salary_info.get("to")
            currency = salary_info.get("currency", "руб.")

            if salary_from and salary_to:
                salary_str = f"{salary_from}–{salary_to} {currency}"
            elif salary_from:
                salary_str = f"от {salary_from} {currency}"
            elif salary_to:
                salary_str = f"до {salary_to} {currency}"

        return cls(
            title=item["name"],
            url=item["alternate_url"],
            salary=salary_str,
            description=item.get("snippet", {}).get("requirement", "") or "",
            employer=item.get("employer", {}).get("name", ""),
            published_at=item.get("published_at", ""),
        )

    # Магические методы сравнения (по зарплате)
    def __lt__(self, other: "Vacancy") -> bool:
        return self.get_salary_value() < other.get_salary_value()

    def __le__(self, other: "Vacancy") -> bool:
        return self.get_salary_value() <= other.get_salary_value()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.get_salary_value() == other.get_salary_value()

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __gt__(self, other: "Vacancy") -> bool:
        return self.get_salary_value() > other.get_salary_value()

    def __ge__(self, other: "Vacancy") -> bool:
        return self.get_salary_value() >= other.get_salary_value()

    # Строковые представления
    def __str__(self) -> str:
        return f"{self.title} ({self.salary}) — {self.url}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"title='{self.title}', url='{self.url}', "
            f"salary='{self.salary}', employer='{self.employer}', "
            f"published_at='{self.published_at}')"
        )

    @staticmethod
    def print_vacancies(vacancies: List["Vacancy"]) -> None:
        """
        Выводит информацию о вакансиях в удобной форме.

        Args:
            vacancies (List[Vacancy]): Список вакансий для вывода.
        """
        logger.info("Вывод в консоль информации о вакансиях")
        for i, vacancy in enumerate(vacancies, start=1):
            print(f"{i}. {vacancy.title}")
            print(f"Зарплата: {vacancy.salary}")
            print(f"Описание: {vacancy.description[:300]}...")  # Ограничили вывод первых 100 символов
            print(f"Работодатель: {vacancy.employer}")
            print(f"Ссылка: {vacancy.url}")
            print("-" * 50)
