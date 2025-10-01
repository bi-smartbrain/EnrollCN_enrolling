import time
import threading
from datetime import datetime, time as dt_time
from config import Config
from enroll_processor import EnrollProcessor
import color_prints as p
from logger import setup_logger

logger = setup_logger()


class Scheduler:
    def __init__(self, functions_module):
        self.processor = EnrollProcessor(functions_module)
        self.functions = functions_module
        self.is_running = True

    def should_run_now(self):
        """Проверяем, нужно ли запускать сейчас"""
        now = datetime.now()

        # Проверяем день недели
        if now.weekday() not in Config.WORKING_DAYS:
            return False

        # Проверяем время
        current_time = now.time()
        target_time = Config.SCHEDULE_TIME

        # Запускаем в течение 1 минуты после целевого времени
        time_diff = abs((current_time.hour * 60 + current_time.minute) -
                        (target_time.hour * 60 + target_time.minute))
        return time_diff <= 1

    def run_scheduled(self):
        """Запуск по расписанию"""
        p.print_info("🕐 Запуск по расписанию...")
        success, message = self.processor.process_enrollment()

        if not success:
            p.print_error(f"Скрипт завершился с критической ошибкой: {message}")
            if Config.RESTART_ON_CRITICAL_ERROR:
                p.print_info("Пытаемся перезапустить...")
                self._restart_on_error()
            else:
                p.print_info("Ожидаем следующего рабочего дня для повторного запуска")

    def _restart_on_error(self):
        """Перезапуск при критической ошибке"""
        try:
            # Ждем 5 минут перед перезапуском
            time.sleep(300)
            p.print_info("🔄 Перезапуск после критической ошибки...")
            self.run_scheduled()
        except Exception as e:
            p.print_error(f"Ошибка при перезапуске: {str(e)}")
            logger.error(f"🔴 Ошибка при перезапуске: {str(e)}")

    def run_once(self):
        """Ручной запуск"""
        p.print_info("▶️ Ручной запуск скрипта энроллинга...")
        logger.info("▶️ Ручной запуск скрипта энроллинга")

        success, message = self.processor.process_enrollment()

        if success:
            p.print_success(f"Скрипт завершил работу: {message}")
        else:
            p.print_error(f"Скрипт завершился с ошибкой: {message}")

        return success

    def check_schedule(self):
        """Проверяет расписание и запускает задачи"""
        last_run_day = None

        while self.is_running:
            try:
                now = datetime.now()
                today_str = now.strftime("%Y-%m-%d")

                # Проверяем, нужно ли запускать сейчас
                if (self.should_run_now() and
                        last_run_day != today_str and
                        now.weekday() in Config.WORKING_DAYS):
                    p.print_info(f"🕐 Обнаружено время запуска: {Config.SCHEDULE_TIME}")
                    self.run_scheduled()
                    last_run_day = today_str

                time.sleep(30)  # Проверяем каждые 30 секунд

            except Exception as e:
                p.print_error(f"Ошибка в планировщике: {str(e)}")
                logger.error(f"🔴 Ошибка в планировщике: {str(e)}")
                time.sleep(60)

    def start_scheduler(self):
        """Запуск планировщика в отдельном потоке"""
        p.print_success(f"Скрипт энроллинга запущен. Ожидание {Config.SCHEDULE_TIME} в рабочие дни...")
        logger.info(f"🟢 Скрипт энроллинга запущен. Расписание: {Config.SCHEDULE_TIME} Пн-Пт")

        # Запускаем проверку расписания в отдельном потоке
        scheduler_thread = threading.Thread(target=self.check_schedule, daemon=True)
        scheduler_thread.start()

        try:
            # Главный поток ждет завершения (или Ctrl+C)
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            p.print_info("Скрипт остановлен пользователем")
            logger.info("⏹️ Скрипт энроллинга остановлен пользователем")
            self.is_running = False

    def stop(self):
        """Остановка планировщика"""
        self.is_running = False
