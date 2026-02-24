import os.path
import sqlite3
import time
from contextlib import contextmanager
from sqlite3 import Connection, OperationalError
from typing import Optional, List, Tuple

from loguru import logger


@contextmanager
def sqlite_timelimit(conn: Connection, ms):
    # logger.trace(f"[{os.getpid()}]: Still Executing query!!")
    deadline = time.perf_counter() + (ms / 1000)
    n = 1000
    if ms <= 20:
        n = 1

    def handler():
        if time.perf_counter() >= deadline:
            return 1

    conn.set_progress_handler(handler, n)
    try:
        yield
    finally:
        conn.set_progress_handler(None, n)
        conn.close()


class SqliteFacade:
    def __init__(self, db_dir: str):
        self.db_dir = db_dir

    def check_connection(self):
        logger.info("Checking SQLite connection!")

    def exec_query(self, db_id: str, sql: str) -> Optional[List[Tuple]]:

        db_file = os.path.join(self.db_dir, db_id, f"{db_id}.sqlite")
        try:
            conn = sqlite3.connect(f"file:{db_file}?mode=ro")
        except sqlite3.OperationalError as e:
            if "unable to open database file" in str(e):
                logger.error(f"SQLite Error: {db_id} {db_file}")
            else:
                logger.error(f"SQLite Error: {db_id} {sql}")
            raise
        logger.debug(f"Connection created")

        with sqlite_timelimit(conn, 60000):
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                logger.debug(f"Query executed")

                rows = cursor.fetchall()
                logger.debug(f"Result fetched")
            except OperationalError as e:
                if e.args == ('interrupted',):
                    logger.warning(f"SQLite Timed out: {db_id} {sql}")
                else:
                    logger.error(f"SQLite Error: {e}")
                rows = None
            finally:
                cursor.close()
            return rows
