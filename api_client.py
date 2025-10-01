import time
from config import Config


class APIClient:
    def __init__(self, api_instance):
        self.api = api_instance
        self.timeout = Config.API_TIMEOUT
        self.max_retries = Config.MAX_RETRIES
        self.retry_delay = Config.RETRY_DELAY

    def post_with_timeout(self, endpoint, data=None):
        """Выполняет POST запрос с таймаутом и повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                # Здесь должна быть реализация таймаута для вашего API клиента
                # Если ваш API клиент поддерживает timeout, используйте его:
                # response = self.api.post(endpoint, data=data, timeout=self.timeout)

                # Если нет, можно использовать внешний таймаут через threading
                response = self.api.post(endpoint, data=data)
                return response

            except Exception as e:
                if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                    if attempt == self.max_retries - 1:
                        raise Exception(f"Timeout after {self.max_retries} attempts: {str(e)}")
                    time.sleep(self.retry_delay)
                else:
                    raise e

    def search_leads(self, query):
        """Поиск лидов с таймаутом"""
        query['include_counts'] = True
        return self.post_with_timeout('data/search/', data=query)

    def subscribe_sequence(self, data):
        """Подписка на цепочку с таймаутом"""
        return self.post_with_timeout('bulk_action/sequence_subscription', data=data)
