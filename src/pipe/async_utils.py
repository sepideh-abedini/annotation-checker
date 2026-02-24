import asyncio
import os

from loguru import logger
from tqdm.asyncio import tqdm

ASYNC_BATCH = int(os.environ.get("ASYNC_BATCH", 1))


class TqdmLogger:
    def write(self, msg):
        msg = msg.strip()
        if msg:
            logger.info(msg)

    def flush(self):
        pass

tqdm_logger = TqdmLogger()

async def apply_async(fun, items, desc=""):
    semaphore = asyncio.Semaphore(ASYNC_BATCH)

    async def sem_task(item):
        async with semaphore:
            return await fun(item)

    tasks = [asyncio.create_task(sem_task(item)) for item in items]
    import sys
    results = await tqdm.gather(*tasks, total=len(items), desc=desc,file=tqdm_logger)
    return results
