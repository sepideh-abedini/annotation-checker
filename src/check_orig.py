import argparse
import asyncio
import os
import re
from typing import Dict

from dotenv import load_dotenv

from src.a2_orig_mask import mask_questions
from src.json_utils import write_json
from src.pipe.pipeline import Pipeline
from src.util.sqlite_facade import SqliteFacade

CHUNKS_DIR = "chunks"

load_dotenv()

from src.pipe.processor.list_transformer import JsonListTransformer
from src.util.log_utils import configure_logging


class DebugMaskedQuestion(JsonListTransformer):
    def __init__(self):
        super().__init__()
        self.dbf = SqliteFacade("data/bird/databases")

    async def _process_row(self, row: Dict) -> Dict:
        print("#" * 100)
        print(f"{'IDX':<12}: {row['idx']}")
        orig_db_id = row['db_id']
        orig_question = row['question']
        orig_masked = row['masked_question_norm']
        print(f"{'Masked[O]':<12}: {orig_masked}")
        print(f"{'Question[O]':<12}: {orig_question}")
        print(f"{'DB ID[O]':<12}: {orig_db_id}")
        return row


class MaskedHash(JsonListTransformer):
    def mask(self, row, src, dst):
        symb = self.get_prop(row, src)
        masked = re.sub(r'\[(\D+)\d+\]', r'[\1]', symb)
        self.set_prop(row, dst, masked)

    async def _process_row(self, row: Dict) -> Dict:
        self.mask(row, "symbolic.question", "masked_question_norm")
        return row


async def check_alt_masking(merged_questions_path):
    configure_logging()
    pipeline = Pipeline([
        MaskedHash(),
        DebugMaskedQuestion()
    ], [])
    await pipeline.run(merged_questions_path, force=True)


KEYS = ["idx", "db_id", "question", "query", "masked_question", "alt_db_id", "alt_question", "alt_masked_questions"]


async def main(dataset, chunk):
    tables_path = os.path.join("data", dataset, "tables.json")
    orig_questions_path = os.path.join("data", dataset, "questions.orig.json")
    orig_data = await mask_questions(orig_questions_path, tables_path, cache_dir="cache/orig")
    masked_hash = MaskedHash()
    debugger = DebugMaskedQuestion()
    for idx, row in orig_data.items():
        await masked_hash._process_row(row)
        await debugger._process_row(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", choices=["example_bird", "example_spider", "bird"], required=True)
    parser.add_argument("-c", required=False)
    args = parser.parse_args()
    dataset = args.d
    chunk = args.c
    asyncio.run(main(dataset, chunk))
