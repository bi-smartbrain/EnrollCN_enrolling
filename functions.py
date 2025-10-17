from closeio_api import Client
import gspread
from gspread.utils import rowcol_to_a1
from env_loader import SECRETS_PATH
import os

SERVICE_ACCOUNT_FILE = os.path.join(SECRETS_PATH, 'service_account.json')
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
api_key = os.getenv('CLOSE_API_KEY_MARY')
api = Client(api_key)


def get_sheet_titles(spreadsheet_name):
    # Открытие таблицы по названию
    spreadsheet = gc.open(spreadsheet_name)

    # Получение списка всех листов и их названий
    sheet_titles = [sheet.title for sheet in spreadsheet.worksheets()]

    return sheet_titles


def find_sequence_by_name(sequence_name):
    target_seq = None
    skip = 0
    flag = True
    while flag:
        params = {'_skip': skip}
        resp = api.get('sequence', params=params)
        seqs = resp['data']
        skip += 100
        for seq in seqs:
            if seq['name'].lower() == sequence_name.strip().lower():
                target_seq = seq
                break
        if resp['has_more'] == False:
            flag = False
    return target_seq


def find_emailacct_by_email(email):
    emailaccts = api.get('connected_account')['data']
    filtered_accts = [acct for acct in emailaccts if acct.get('email').lower() == email.lower().strip()]
    target_acct = None
    for acct in filtered_accts:
        if acct.get('send_status') == 'ok':
            target_acct = acct
            break
    return target_acct


def get_sheet_range(spread, income_sheet, income_range):
    """Получает из гугл-таблицы диапазон"""
    sh = gc.open(spread)
    data = sh.worksheet(income_sheet).get(income_range)
    return data


def add_report_to_sheet(spread, sheet, report):
    """
    Добавляет на лист данные отчета без удаления уже существующих там записей
    :param spread: гугл таблица (название)
    :param sheet: название листа
    :param report: отчет в виде списка списков
    :return: None
    """
    sh = gc.open(spread)
    worksheet = sh.worksheet(sheet)

    # Получить размеры отчета (количество строк и столбцов)
    num_rows = len(report)
    num_cols = len(report[0])

    # Получить диапазон для записи данных
    q_rows = len(worksheet.get_all_values())  # узнаем кол-во уже заполненных на листе строк

    start_cell = rowcol_to_a1(q_rows + 1, 1)
    end_cell = rowcol_to_a1(q_rows + num_rows, num_cols)

    # Записать значения в диапазон
    cell_range = f"{start_cell}:{end_cell}"
    worksheet.update(cell_range, report, value_input_option="USER_ENTERED")

    print("Отчет добавлен")


def write_spread_sheet(spread, sheet, report):
    """
    Очищает лист гугл таблицы и записывает на него отчет
    :param spread: гугл таблица (название)
    :param sheet: название листа
    :param report: отчет в виде списка списков
    :return: None
    """
    sh = gc.open(spread)
    worksheet = sh.worksheet(sheet)
    worksheet.clear()
    print(f"Лист {sheet} в таблице {spread} очищен")

    # Получить размеры отчета (количество строк и столбцов)
    num_rows = len(report)
    num_cols = len(report[0])

    # Получить диапазон для записи данных
    start_cell = rowcol_to_a1(1, 1)
    end_cell = rowcol_to_a1(num_rows, num_cols)

    # Записать значения в диапазон
    cell_range = f"{start_cell}:{end_cell}"
    worksheet.update(report, cell_range, value_input_option="user_entered")
