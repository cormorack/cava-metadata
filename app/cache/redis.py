from redis.exceptions import ConnectionError
import redis.asyncio as aioredis
from typing import Optional
from core.config import settings
import time
import logging

logging.root.setLevel(level=logging.INFO)
logger = logging.getLogger('uvicorn')


class RedisDependency:
    """FastAPI Dependency for Redis Connections"""

    redis: Optional[aioredis.client.Redis] = None
    connected: bool = False
    retries: int = 1

    async def __call__(self) -> Optional[aioredis.client.Redis]:
        if self.redis is None:
            await self.init()
        if self.connected:
            return self.redis

    async def init(self):
        """Initialises the Redis Dependency"""
        logger.info("Starting redis connection ...")
        self.redis = await aioredis.from_url(str(settings.REDIS_URI))

        while not self.connected:
            try:
                await self.redis.ping()
                self.connected = True
                logger.info("Redis connected!")
            except ConnectionError:
                if self.retries > 0:
                    logger.warning("Not connected to Redis. Trying again.")
                    time.sleep(5)
                    self.retries = self.retries - 1
                else:
                    logger.warning("Retries Exceeded. No caching available.")
                    break


redis_dependency: RedisDependency = RedisDependency()
