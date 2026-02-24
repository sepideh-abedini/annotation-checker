import json
import os
from typing import List, Dict

from src.json_utils import write_json
from src.pipe.processor.list_transformer import JsonListTransformer

START = int(os.environ.get("START", 0))
LIMIT = int(os.environ.get("LIMIT", 10))


class LimitJson(JsonListTransformer):

    async def run(self, input_file):
        output_file = self.get_output_file(input_file)

        with open(input_file) as f:
            in_data = json.load(f)

        out_data = in_data[START:START + LIMIT]

        out_rows = []
        for row in out_data:
            out_rows.append(row)

        with open(output_file, "w") as f:
            f.write(json.dumps(out_rows, indent=4))
        return output_file, out_rows

    async def _process_row(self, row):
        return row


class FilterList(JsonListTransformer):
    def __init__(self, predicate=lambda r: r):
        super().__init__()
        self.predicate = predicate

    async def run(self, input_file):
        output_file = self.get_output_file(input_file)

        with open(input_file) as f:
            in_data = json.load(f)

        out_data = []
        for row in in_data:
            if self.predicate(row):
                out_data.append(row)

        with open(output_file, "w") as f:
            f.write(json.dumps(out_data, indent=4))
        return output_file, out_data

    async def _process_row(self, row):
        return row


class KeepProps(JsonListTransformer):
    def __init__(self, props: List[str]):
        super().__init__()
        self.props = props

    async def _process_row(self, row):
        filtered_row = dict()
        for p in self.props:
            filtered_row[p] = row[p]
        return filtered_row


class SaveAs(JsonListTransformer):
    def __init__(self, save_path: str):
        super().__init__()
        self.save_path = save_path
        self.rows = []

    def _post_run(self):
        write_json(self.save_path, self.rows)

    async def _process_row(self, row):
        self.rows.append(row)
        return row

class CollectUnique(JsonListTransformer):
    def __init__(self, extractor):
        super().__init__()
        self.collection = set()
        self.extractor = extractor

    def _post_run(self):
        print(self.collection)

    async def _process_row(self, row: Dict) -> Dict:
        data = self.extractor(row)
        self.collection.add(data)
        return row
