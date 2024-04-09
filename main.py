from utils import create_table, add_to_table

from db_manager import DBManager


def main():
    employers_list = [3127, 1740, 15478, 8620, 3776,3529, 78638, 4006, 4504679, 561525, 64174, 4181, 8642172]
    dbmanager = DBManager()
    create_table()
    add_to_table(employers_list)


    while True:

        task = input(
            "Введите 1, получить список всех компаний и количество вакансий у каждой компании\n"
            "Введите 2, получить список всех вакансий с указанием названия компании, "
            "названия вакансии и зарплаты и ссылки на вакансию\n"
            "Введите 3, получить среднюю зарплату по вакансиям\n"
            "Введите 4, получить список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
            "Введите 5, получить список всех вакансий, в названии которых содержатся переданные в метод слова\n"
            "Введите стоп, завершить работу\n".lower())

        if task == "стоп":
            break
        elif task == '1':
            print(dbmanager.get_companies_and_vacancies_count())
            print()
        elif task == '2':
            print(dbmanager.get_all_vacancies())
            print()
        elif task == '3':
            print(dbmanager.get_avg_salary())
            print()
        elif task == '4':
            print(dbmanager.get_vacancies_with_higher_salary())
            print()
        elif task == '5':
            keyword = input('Введите ключевое слово: ')
            print(dbmanager.get_vacancies_with_keyword(keyword))
            print()
        else:
            print('Неправильный запрос')


main()