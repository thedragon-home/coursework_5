import psycopg2
import requests


def get_vacancies(employer_id):
    """Получение данных вакансий по API"""

    params = {
        'area': 1,
        'page': 0,
        'per_page': 10
    }
    url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
    data_vacancies = requests.get(url, params=params).json()

    vacancies_data = []
    for item in data_vacancies["items"]:
        hh_vacancies = {
            'vacancy_id': int(item['id']),
            'vacancies_name': item['name'],
            'payment': item["salary"]["from"] if item.get("salary") else None,
            'requirement': item['snippet']['requirement'],
            'vacancies_url': item['alternate_url'],
            'employer_id': employer_id
        }
        if hh_vacancies['payment'] is not None:
            vacancies_data.append(hh_vacancies)

    return vacancies_data


def get_employer(employer_id):
    """Получаем данные о работодателях по API"""

    url = f"https://api.hh.ru/employers/{employer_id}"
    data_vacancies = requests.get(url).json()
    hh_company = {
        "employer_id": int(employer_id),
        "company_name": data_vacancies['name'],
        "open_vacancies": data_vacancies['open_vacancies']
    }

    return hh_company


def get_connection(database="company"):
    """Получение соединения с базой данных"""

    return psycopg2.connect(host="localhost", database=database, user="postgres", password="12345")


def create_table():
    """Создание БД, создание таблиц"""

    conn = psycopg2.connect(host="localhost", database="company", user="postgres", password="12345")
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = 'company';")
    except psycopg2.OperationalError as e:
        print('')

    conn.close()

    conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="12345")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS company")
    cur.execute("CREATE DATABASE company")

    cur.close()
    conn.close()


    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE employers (
                        employer_id INTEGER PRIMARY KEY,
                        company_name varchar(255),
                        open_vacancies INTEGER
                        )""")

            cur.execute("""
                        CREATE TABLE vacancies (
                        vacancy_id SERIAL PRIMARY KEY,
                        vacancies_name varchar(255),
                        payment INTEGER,
                        requirement TEXT,
                        vacancies_url TEXT,
                        employer_id INTEGER REFERENCES employers(employer_id)
                        )""")


def add_to_table(employers_list):
    """Заполнение базы данных компании и вакансии"""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')

            for employer in employers_list:
                employer_list = get_employer(employer)
                cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                            'VALUES (%s, %s, %s) RETURNING employer_id',
                            (employer_list['employer_id'], employer_list['company_name'],
                             employer_list['open_vacancies']))

            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                for v in vacancy_list:
                    cur.execute('INSERT INTO vacancies (vacancy_id, vacancies_name, '
                                'payment, requirement, vacancies_url, employer_id) '
                                'VALUES (%s, %s, %s, %s, %s, %s)',
                                (v['vacancy_id'], v['vacancies_name'], v['payment'],
                                 v['requirement'], v['vacancies_url'], v['employer_id']))

        conn.commit()
