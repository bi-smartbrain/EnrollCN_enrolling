import functions as f
from scheduler import Scheduler
import color_prints as p
from logger import setup_logger

logger = setup_logger()


def main():
    """Основная функция"""
    try:
        p.print_success("Инициализация скрипта энроллинга...")

        # Создаем и запускаем планировщик
        scheduler = Scheduler(f)

        # Запуск планировщика
        scheduler.start_scheduler()

    except Exception as e:
        logger.error(f"🔴 Enroll_CN_enrolling.py Критическая ошибка при запуске: {str(e)}")


if __name__ == '__main__':
    main()
