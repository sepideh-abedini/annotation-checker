import json
from abc import ABC, abstractmethod
from typing import Dict

from src.pipe.async_utils import apply_async


class JsonListProcessor(ABC):
    async def __process_row_internal(self, row: Dict) -> Dict:
        # try:
        return await self._process_row(row)
        # except Exception as e:
        #     idx = row.get("idx", "unknown")
        #     logger.error(f"Error processing row {idx}: {e}")
        #     return row

    @abstractmethod
    async def _process_row(self, row: Dict) -> Dict:
        pass

    def get_prop(self, row, prop):
        props = prop.split(".")
        d = row
        for p in props:
            d = d[p]
        return d

    def set_prop(self, row, prop, value):
        props = prop.split(".")
        d = row
        for p in props[:-1]:
            if p not in d:
                d[p] = {}
            d = d[p]
        d[props[-1]] = value
        return row

    @property
    def name(self):
        return self.__class__.__name__

    def _pre_run(self, input_file):
        pass

    def _post_run(self):
        pass

    def _post_run_file(self, input_file):
        pass

    def _get_input_data(self, input_file):
        with open(input_file) as f:
            in_data = json.load(f)
            return in_data

    async def run(self, input_file):
        in_data = self._get_input_data(input_file)

        self._pre_run(input_file)

        output = await apply_async(self.__process_row_internal, in_data, self.name)

        self._post_run()


        return output
