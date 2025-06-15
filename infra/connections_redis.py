import redis
import json
import atexit
from abc import ABC
from typing import List


class RedisConnector(ABC):
    """Redis connector abstract class"""

    def __init__(self, host: str, port: int, db: int ) -> None:
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        atexit.register(self.close)
        return

    def get_all_keys(self) -> None:
        return self.client.keys()

    def flushdb(self) -> None:
        print("Flushing Redis DB")
        self.client.flushdb()
        return

    def close(self) -> None:
        self.client.close()
        return


class RedisQueue(RedisConnector):
    """Redis queue connector"""

    def flush_queue(self, queue: str) -> None:
        self.client.delete(queue)
        return

    def get_llen(self, queue: str):
        return self.client.llen(queue)

    def batch_lpush(self, queue: str, records: List[str]):
        """add to the head of a list"""
        if not records:  # Don't attempt to push empty list
            return
        with self.client.pipeline() as pipeline:
            for row in records:
                pipeline.lpush(queue, str(json.dumps(row)))
            pipeline.execute()

    def batch_rpush(self, queue: str, records: List[str]):
        """add to the tail of a list"""
        if not records:  # Don't attempt to push empty list
            return
        with self.client.pipeline() as pipeline:
            for row in records:
                pipeline.rpush(queue, str(json.dumps(row)))
            pipeline.execute()

    def batch_lpop(self, queue: str, count: int) -> list:
        """remove from the head of a list"""
        entries = self.client.lpop(queue, count=count)
        if entries is None:
            return []
        return [json.loads(entry) for entry in entries]

    def batch_rpop(self, queue: str, count: int) -> list:
        """remove from the tail of a list"""
        entries = self.client.rpop(queue, count=count)
        if entries is None:
            return []
        return [json.loads(entry) for entry in entries]

    def batch_lrem(self, queue: str, values: list, count: int = 0):
        """Remove a list of values from the specified queue"""
        with self.client.pipeline() as pipeline:
            for value in values:
                pipeline.lrem(queue, count, str(json.dumps(value)))
            pipeline.execute()
