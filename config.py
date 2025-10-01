from datetime import time


class Config:
    # Основные настройки
    SPREAD_NAME = "Rubrain - Enroll CN"
    SHEET_PREFIX = "111_"

    # Расписание
    SCHEDULE_TIME = time(19, 50)  # 13:00
    WORKING_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт (0-пн, 6-вс)

    # Настройки API
    API_TIMEOUT = 30  # секунд
    MAX_RETRIES = 3
    RETRY_DELAY = 60  # секунд

    # Настройки подписок
    SUBSCRIPTION_DELAY_MIN = 2  # секунд
    SUBSCRIPTION_DELAY_MAX = 5  # секунд

    # Логика ошибок
    ERROR_THRESHOLD = 0.9  # 90% ошибок - критический уровень
    RESTART_ON_CRITICAL_ERROR = False  # Перезапуск при критической ошибке

    # Логирование
    LOG_TO_FILE = True
    LOG_FILE = "enroll_processor.log"
    LOG_LEVEL = "INFO"
