import os
from abc import ABC
from typing import Dict

from src.pipe.processor.list_processor import JsonListProcessor

FORCE = int(os.environ.get("FORCE", 0)) > 0


class ReportStep(JsonListProcessor, ABC):
    result: Dict

    def __init__(self):
        super().__init__()
        self.result = {}
        self.__runned = False

    def _pre_run(self, input_file):
        if self.__runned:
            raise RuntimeError("Called TWICE!")
        self.__runned = True

    def get_result(self):
        return self.result
