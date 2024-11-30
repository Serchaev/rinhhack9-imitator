import json
import logging

from redis.asyncio.client import Redis
import pandas as pd

from app.config import settings, get_logger


class ImitatorTransactions:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.df = pd.read_csv('config/dataset.csv')
        self.index = 0
        self.logger = get_logger(__name__)

    async def _send_to_queue(self, record: dict):
        record_json = json.dumps(record)

        await self.redis.rpush(settings.AMQP.routing_keys.model_manager_routing_key, record_json)

    def _select_one_transaction(self):
        return self.df.iloc[self.index].to_dict()

    async def emulate(self):
        try:
            if self.index < 0 or self.index >= len(self.df):
                self.index = 0

            record = self._select_one_transaction()
            self.index += 1

            await self._send_to_queue(record=record)
        except Exception as e:
            self.logger.exception(f"Ошибка эмулирования данных --- {e}")
        else:
            self.logger.info(f"Данные отправлены в редис --- id({self.index})")



