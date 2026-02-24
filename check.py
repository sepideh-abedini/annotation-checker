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


def cprin(msg, color="green"):
    colors = {
        "black": 30, "red": 31, "green": 32, "yellow": 33,
        "blue": 34, "magenta": 35, "cyan": 36, "white": 37
    }
    code = colors.get(color, 37)
    print(f"\033[{code}m{msg}\033[0m")


class DebugMaskedQuestion(JsonListTransformer):
    def __init__(self):
        super().__init__()
        self.dbf = SqliteFacade("data/bird/databases")

    async def _process_row(self, row: Dict) -> Dict:
        print("#" * 100)
        print(f"{'IDX':<12}: {row['idx']}")
        if "alt_masked_question_norm" not in row:
            cprin(f"{'Invalid annotation':<12}", "red")
            return row
        orig_db_id = row['db_id']
        alt_db_id = row['alt_db_id']

        gold_sql = row['alt_sql']
        exec_res = self.dbf.exec_query(alt_db_id, gold_sql)

        alt_question = row['alt_question']
        orig_question = row['question']
        orig_masked = row['masked_question_norm']
        alt_masked = row['alt_masked_question_norm']
        print(f"{'DB ID[A]':<12}: {alt_db_id}")
        print(f"{'Question[A]':<12}: {alt_question}")
        cprin(f"{'Masked[A]':<12}: {alt_masked}", "green" if alt_masked == orig_masked else "red")
        print(f"{'Masked[O]':<12}: {orig_masked}")
        print(f"{'Question[O]':<12}: {orig_question}")
        print(f"{'DB ID[O]':<12}: {orig_db_id}")
        print("------")
        print(f"{'SQL [A]':<12}: {gold_sql}")
        print(f"{'Res [A]':<12}: {exec_res[:15]}")
        return row


class MaskedHash(JsonListTransformer):
    def mask(self, row, src, dst):
        symb = self.get_prop(row, src)
        masked = re.sub(r'\[(\D+)\d+\]', r'[\1]', symb)
        self.set_prop(row, dst, masked)

    async def _process_row(self, row: Dict) -> Dict:
        if "alt_masked_question" not in row:
            return row
        self.mask(row, "masked_question", "masked_question_norm")
        self.mask(row, "alt_masked_question", "alt_masked_question_norm")
        return row


async def check_alt_masking(merged_questions_path, tables_path):
    configure_logging()
    pipeline = Pipeline([
        MaskedHash(),
        DebugMaskedQuestion()
    ], [])
    await pipeline.run(merged_questions_path, force=True)


KEYS = ["idx", "db_id", "question", "query", "masked_question", "alt_db_id", "alt_question", "alt_masked_questions"]


async def add_alt_data(orig_questions_path, alt_questions_path, tables_path):
    orig_data = await mask_questions(orig_questions_path, tables_path, cache_dir="cache/orig")
    alt_data = await mask_questions(alt_questions_path, tables_path, cache_dir="cache/alt")
    merged_rows = []
    for idx, alt_row in alt_data.items():
        assert idx in orig_data, f"idx not found in orig: {idx}"
        orig_row = orig_data[idx]
        orig_masked = orig_row['symbolic']['question']
        alt_db_id = alt_row["db_id"]
        alt_question = alt_row["question"]
        alt_sql = alt_row["gold_sql"]
        for key in list(orig_row.keys()):
            if key not in KEYS:
                del orig_row[key]
        orig_row['masked_question'] = orig_masked
        orig_row['alt_db_id'] = alt_db_id
        orig_row['alt_question'] = alt_question
        orig_row['alt_sql'] = alt_sql

        if "symbolic" in alt_row:
            orig_row['alt_masked_question'] = alt_row["symbolic"]["question"]

        merged_rows.append(orig_row)
    os.makedirs("cache/checker", exist_ok=True)
    output_path = os.path.join("cache", "checker", "questions.json")
    write_json(output_path, list(merged_rows))
    return output_path


async def main(dataset, chunk):
    orig_questions_path = os.path.join("data", dataset, "questions.orig.json")
    if chunk:
        alt_questions_path = os.path.join("data", dataset, CHUNKS_DIR, f"chunk_{chunk}.json")
    else:
        alt_questions_path = os.path.join("data", dataset, "questions.alt.json")
    tables_path = os.path.join("data", dataset, "tables.json")
    out_path = await add_alt_data(orig_questions_path, alt_questions_path, tables_path)
    await check_alt_masking(out_path, tables_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", choices=["example_bird", "example_spider", "bird"], required=True)
    parser.add_argument("-c", required=False)
    args = parser.parse_args()
    dataset = args.d
    chunk = args.c
    asyncio.run(main(dataset, chunk))
