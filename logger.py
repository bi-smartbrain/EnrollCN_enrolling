import os
from loguru import logger
from notifiers.logging import NotificationHandler
from env_loader import SECRETS_PATH
from config import Config

# Настройка Telegram логгера
token = os.getenv("TG_TOKEN")
chat_id = os.getenv("CHAT_ID_1")

if token and chat_id:
    params_chat = {
        "token": token,
        "chat_id": chat_id,
    }
    tg_handler = NotificationHandler("telegram", defaults=params_chat)
    logger.add(tg_handler, level="DEBUG")

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
