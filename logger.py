import os
from loguru import logger
from notifiers.logging import NotificationHandler
from env_loader import SECRETS_PATH
from config import Config

# Настройка Telegram логгера
token = os.getenv("TG_TOKEN")
chat_id_1 = os.getenv("CHAT_ID_1")
chat_id_4 = os.getenv("CHAT_ID_4")

if token:
    params_chat_1 = {
        "token": token,
        "chat_id": chat_id_1,
    }
    tg_handler_1 = NotificationHandler("telegram", defaults=params_chat_1)
    logger.add(tg_handler_1, level="DEBUG")

    params_chat_4 = {
        "token": token,
        "chat_id": chat_id_4,
    }
    tg_handler_4 = NotificationHandler("telegram", defaults=params_chat_4)
    logger.add(tg_handler_4, level="INFO")


# Настройка файлового логгера
if Config.LOG_TO_FILE:
    logger.add(
        Config.LOG_FILE,
        level=Config.LOG_LEVEL,
        rotation="1 day",  # Ротация каждый день
        retention="7 days",  # Хранить 7 дней
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )

def setup_logger():
    """Настройка логгера"""
    return logger
