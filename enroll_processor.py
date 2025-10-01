import json
import time
import random
import pandas as pd
from datetime import datetime as dt
import color_prints as p
from config import Config
from api_client import APIClient
from logger import setup_logger

logger = setup_logger()


class EnrollProcessor:
    def __init__(self, functions_module):
        self.f = functions_module
        self.api_client = APIClient(functions_module.api)
        self.last_run_date = None

    def should_run_today(self):
        """Проверяем, нужно ли запускать скрипт сегодня"""
        today = dt.now().weekday()
        if today not in Config.WORKING_DAYS:
            p.print_warning(f"Сегодня выходной день, скрипт не запускается")
            return False

        today_str = dt.now().strftime("%Y-%m-%d")
        if self.last_run_date == today_str:
            p.print_warning(f"Скрипт уже запускался сегодня {today_str}")
            return False

        return True

    def load_enrolling_data(self):
        """Загружаем данные для энроллинга"""
        p.print_info("Загрузка данных для энроллинга...")

        enrolling_sheets = self.f.get_sheet_titles(Config.SPREAD_NAME)
        enrolling_reg = pd.DataFrame(columns=['email', 'url', 'filters_json', 'seq_name', 'sheet_name'])

        for sheet_name in enrolling_sheets:
            if Config.SHEET_PREFIX in sheet_name:
                sheet_range = self.f.get_sheet_range(
                    spread=Config.SPREAD_NAME,
                    income_sheet=sheet_name,
                    income_range="A:C"
                )
                if not sheet_range:
                    p.print_warning(f"Пустой лист: {sheet_name}")
                    continue

                seq_name = sheet_range[0][0]
                sheet_reg = pd.DataFrame(sheet_range[3:])
                sheet_reg = sheet_reg.iloc[:, :3]
                sheet_reg.columns = ['email', 'url', 'filters_json']
                sheet_reg['seq_name'] = seq_name
                sheet_reg['sheet_name'] = sheet_name
                enrolling_reg = pd.concat([enrolling_reg, sheet_reg], ignore_index=True)

        email_lists = []
        if email_lists:
            enrolling_reg = enrolling_reg[enrolling_reg['email'].isin(email_lists)]

        p.print_info(f"Загружено {len(enrolling_reg)} записей для обработки")
        return enrolling_reg.to_dict(orient='records')

    def get_sequence_ids(self, enrolling_reg):
        """Получаем ID цепочек"""
        seq_names = {row['seq_name'] for row in enrolling_reg}

        seqID_dict = {}
        for seq_name in seq_names:
            try:
                seq = self.f.find_sequence_by_name(seq_name)
                seq_id = seq['id'] if seq else None
                seqID_dict[seq_name] = seq_id
                if seq_id:
                    p.print_success(f"Найдена цепочка: {seq_name} -> {seq_id}")
                else:
                    p.print_warning(f"Цепочка не найдена: {seq_name}")
            except Exception as e:
                p.print_error(f"Ошибка поиска цепочки {seq_name}: {str(e)}")
                seqID_dict[seq_name] = None

        return seqID_dict

    def process_enrollment(self):
        """Основной процесс энроллинга"""
        if not self.should_run_today():
            return True, "Skipped - not a working day or already run today"

        try:
            enrolling_reg = self.load_enrolling_data()
            if not enrolling_reg:
                p.print_warning("Нет данных для обработки")
                logger.warning("⚠️ Нет данных для обработки энроллинга")
                return True, "No data to process"

            # Уведомление о начале работы
            total_subscriptions = len([r for r in enrolling_reg if r['filters_json']])
            start_message = f"🚀 Начало процесса энроллинга\nЗапланировано подписок: {total_subscriptions}"
            p.print_info(start_message)
            logger.info(start_message)

            seqID_dict = self.get_sequence_ids(enrolling_reg)

            report = [['date_time', 'url', 'sheet', 'seq_name', 'email', 'total_leads', 'bulk_response']]
            success_count = 0
            error_count = 0
            total_count = len([r for r in enrolling_reg if r['filters_json']])

            p.print_info(f"Начинаем обработку {total_count} подписок...")

            for i, row in enumerate(enrolling_reg):
                if not row['filters_json']:
                    continue

                try:
                    result = self.process_single_subscription(row, seqID_dict)
                    report.append(result)

                    if any(error_indicator in str(result[-1]).lower() for error_indicator in
                           ['error', 'не найден', 'exception']) or "нет" in str(result[-2]).lower():
                        error_count += 1
                    else:
                        success_count += 1

                    # Случайная задержка 2-5 секунд
                    if i < total_count - 1:
                        delay = random.uniform(Config.SUBSCRIPTION_DELAY_MIN, Config.SUBSCRIPTION_DELAY_MAX)
                        time.sleep(delay)

                except Exception as e:
                    error_count += 1
                    p.print_error(f"Критическая ошибка при обработке {row['email']}: {str(e)}")
                    date_time = dt.now().strftime("%m/%d/%Y, %H:%M:%S")
                    report.append([
                        date_time,
                        row['url'],
                        row['sheet_name'].replace(Config.SHEET_PREFIX, ''),
                        row['seq_name'],
                        row['email'],
                        f"critical_error: {str(e)}",
                        "Не удалось обработать подписку"
                    ])

            # Анализ результатов и сохранение отчета
            self._save_report(report)
            return self._analyze_results(success_count, error_count, total_count)

        except Exception as e:
            error_msg = f"Критическая ошибка в основном процессе: {str(e)}"
            p.print_error(error_msg)
            logger.error(f"🔴 ENROLLING CRITICAL: {error_msg}")
            return False, f"Process error: {str(e)}"

    def _save_report(self, report):
        """Сохраняет отчет в Google Sheets"""
        if len(report) > 1:
            self.f.add_report_to_sheet(
                spread=Config.SPREAD_NAME,
                sheet='enrolling_pyReport',
                report=report[1:]
            )
            p.print_success("Отчет успешно сохранен")
        else:
            p.print_warning("Нет данных для отчета")

    def _analyze_results(self, success_count, error_count, total_count):
        """Анализирует результаты выполнения"""
        p.print_info(f"Результаты: {success_count} успешно, {error_count} ошибок")

        if total_count > 0:
            error_ratio = error_count / total_count
            if error_ratio >= Config.ERROR_THRESHOLD:
                error_msg = f"КРИТИЧЕСКИЙ УРОВЕНЬ ОШИБОК! Успешно: {success_count}, Ошибок: {error_count} из {total_count} ({error_ratio:.1%})"
                p.print_error(error_msg)
                logger.error(f"🔴 ENROLLING CRITICAL: {error_msg}")

                stats_message = f"📊 Статистика энроллинга (КРИТИЧЕСКИЙ УРОВЕНЬ):\nУспешных подписок: {success_count} из {total_count}\nОшибок: {error_count}"
                logger.error(stats_message)

                return False, f"Critical error threshold reached: {error_ratio:.1%}"
            else:
                stats_message = f"✅ Энроллинг завершен успешно!\nУспешных подписок: {success_count} из {total_count}\nОшибок: {error_count}"
                p.print_success(stats_message)
                logger.info(stats_message)
        else:
            p.print_warning("Нет подписок для обработки")
            logger.warning("⚠️ Нет подписок для обработки")

        self.last_run_date = dt.now().strftime("%Y-%m-%d")
        return True, f"Success: {success_count}/{total_count}"

    def process_single_subscription(self, row, seqID_dict):
        """Обрабатывает одну подписку"""
        p.print_info(f"Обработка: {row['url']} -> {row['email']}")

        query = json.loads(row['filters_json'])

        # Поиск лидов с таймаутом
        try:
            search_response = self.api_client.search_leads(query)
            total_leads = search_response['count']['total']
            p.print_success(f"Найдено лидов: {total_leads}")
        except Exception as e:
            total_leads = f"search_error: {str(e)}"
            p.print_error(f"Ошибка поиска: {str(e)}")

        # Поиск ресурсов
        emailacct = self.f.find_emailacct_by_email(row['email'])
        sequence_id = seqID_dict[row['seq_name']]

        bulk_response = ''

        # Проверка наличия ресурсов
        if not emailacct and not sequence_id:
            bulk_response = f"ящик {row['email']} не найден, цепочка {row['seq_name']} не найдена"
            total_leads = f'НЕТ\n{bulk_response}'
            p.print_error(bulk_response)
        elif not emailacct:
            bulk_response = f"ящик {row['email']} не найден"
            total_leads = f'НЕТ\n{bulk_response}'
            p.print_error(bulk_response)
        elif not sequence_id:
            bulk_response = f"цепочка {row['seq_name']} не найдена"
            total_leads = f'НЕТ\n{bulk_response}'
            p.print_error(bulk_response)
        else:
            # Все ресурсы найдены - выполняем подписку
            sender_name = None
            for item in emailacct['identities']:
                if item['email'].lower() == row['email'].lower():
                    sender_name = item['name']
                    break

            data = {
                "action_type": "subscribe",
                "sequence_id": sequence_id,
                "send_done_email": False,
                "sender_account_id": emailacct['id'],
                "sender_email": emailacct['email'],
                "contact_preference": "lead",
                "s_query": query['query'],
                "sort": query['sort'],
                "results_limit": query['results_limit'],
            }

            if sender_name:
                data["sender_name"] = sender_name

            try:
                resp = self.api_client.subscribe_sequence(data)
                bulk_response = "Успешно"
                p.print_success(f"Подписка выполнена: {row['email']} -> {row['seq_name']}")
            except Exception as e:
                bulk_response = str(e)
                total_leads = f"error\n{bulk_response}"
                p.print_error(f"Ошибка подписки: {str(e)}")

        date_time = dt.now().strftime("%m/%d/%Y, %H:%M:%S")
        return [
            date_time,
            row['url'],
            row['sheet_name'].replace(Config.SHEET_PREFIX, ''),
            row['seq_name'],
            row['email'],
            total_leads,
            bulk_response
        ]
