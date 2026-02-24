import asyncio
import os
import re
import shutil

from dotenv import load_dotenv
from typing import Dict

from src.a2_orig_mask import mask_questions
from src.a3_extract_alt import extract_alts
from src.a4_extract_dbs import extract_dbs
from src.a5_chunk_for_gen import chunk_for_anon
from src.a6_rev_anons import add_anons
from src.json_utils import write_json, read_json, read_json_to_dict
from src.pipe.pipeline import Pipeline

load_dotenv()

from src.pipe.copy_transformer import CopyTransformer
from src.pipe.det_mask import AddSymbolicQuestion
from src.pipe.link_db_id import LinkDbId
from src.pipe.processor.limit_list import FilterList
from src.pipe.processor.list_transformer import JsonListTransformer
from src.pipe.symb_table import AddSymbolTable
from src.util.log_utils import configure_logging


def cprin(msg, color="green"):
    colors = {
        "black": 30, "red": 31, "green": 32, "yellow": 33,
        "blue": 34, "magenta": 35, "cyan": 36, "white": 37
    }
    code = colors.get(color, 37)
    print(f"\033[{code}m{msg}\033[0m")


class DebugMaskedQuestion(JsonListTransformer):
    def __init__(self, orig_path):
        super().__init__()
        self.orig_questions = read_json_to_dict(orig_path, "idx")

    async def _process_row(self, row: Dict) -> Dict:
        masked = row['masked']
        orig_masked = row['orig_masked']
        if masked == orig_masked:
            return row
        print("#" * 100)
        print("IDX:", row['idx'])
        print("DB ID:", row['db_id'])
        print("Question    :", row['question'])
        cprin(f"Masked      : {masked}", "green" if masked == orig_masked else "red")
        print("Masked[O]   :", orig_masked)
        print("Question[O] :", row['original_question'])
        print("REPLACING WITH ORIGINAL")
        orig = self.orig_questions[row['idx']]
        return orig


class MaskedHash(JsonListTransformer):
    def mask(self, row, src, dst):
        symb = self.get_prop(row, src)
        masked = re.sub(r'\[(\D+)\d+\]', r'[\1]', symb)
        self.set_prop(row, dst, masked)

    async def _process_row(self, row: Dict) -> Dict:
        self.mask(row, "symbolic.question", "masked")
        self.mask(row, "original_symbolic", "orig_masked")
        return row


async def remove_unmatched(chunk):
    configure_logging()
    tmp_dir = f"tmp/chunk_{chunk}"
    os.makedirs(tmp_dir, exist_ok=True)
    data_path = os.path.join(tmp_dir, "data.json")
    shutil.copy(f"chunk_rev/alt_questions_{chunk}.json", data_path)
    pipeline = Pipeline([
        FilterList(lambda r: r['question']),
        CopyTransformer("gold_value_links", "value_links"),
        CopyTransformer("gold_schema_links", "schema_links"),
        CopyTransformer("value_links", "filtered_value_links"),
        CopyTransformer("schema_links", "filtered_schema_links"),
        AddSymbolTable("alt_schemas.json"),
        LinkDbId(),
        AddSymbolicQuestion(),
        MaskedHash(),
        DebugMaskedQuestion("questions.json")
    ], [])

    await pipeline.run(data_path, force=True)
    data = read_json(os.path.join(tmp_dir, "output.json"))
    return data


async def main():
    await mask_questions()
    extract_alts()
    extract_dbs()
    chunk_for_anon()
    chunks = os.listdir("chunk_anons")
    all_data = []
    for i, chunk in enumerate(chunks):
        add_anons(i)
        data = await remove_unmatched(i)
        all_data.extend(data)
    write_json("alt_questions.json", all_data)
    extract_dbs()


if __name__ == '__main__':
    asyncio.run(main())
