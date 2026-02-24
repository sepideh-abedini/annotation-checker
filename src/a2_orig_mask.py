import asyncio
import os
import shutil
from typing import Dict

from dotenv import load_dotenv

from src.a1_extract_alt_dbs import extract_alt_dbs
from src.json_utils import read_json_to_dict
from src.pipe.add_schema import AddSchema
from src.pipe.pipeline import Pipeline
from src.pipe.processor.list_transformer import JsonListTransformer

load_dotenv()

from src.pipe.copy_transformer import CopyTransformer
from src.pipe.det_mask import AddSymbolicQuestion
from src.pipe.link_db_id import LinkDbId
from src.pipe.processor.limit_list import FilterList, LimitJson
from src.pipe.symb_table import AddSymbolTable
from src.util.log_utils import configure_logging


class MaskedHash(JsonListTransformer):
    async def _process_row(self, row: Dict) -> Dict:
        masked_terms = row["symbolic"]["masked_terms"]
        question = row["symbolic"]["question"]

        for key, value in masked_terms.items():
            mask = "#" * len(str(key))
            question = question.replace(value, mask)

        row["symbolic"]["question"] = question
        return row


class DebugMaskedQuestion(JsonListTransformer):
    async def _process_row(self, row: Dict) -> Dict:
        print("#" * 100)
        print("IDX:", row['idx'])
        print("DB ID:", row['db_id'])
        print("Question    :", row['question'])
        print("Masked      :", row['symbolic']['question'])
        return row


async def mask_questions(questions_path: str, tables_path: str, cache_dir):
    configure_logging()
    os.makedirs(cache_dir, exist_ok=True)
    data_path = os.path.join(cache_dir, "questions.json")
    shutil.copy(questions_path, data_path)
    pipeline = Pipeline([
        LimitJson(),
        CopyTransformer("gold_value_links", "value_links"),
        CopyTransformer("gold_schema_links", "schema_links"),
        CopyTransformer("value_links", "filtered_value_links"),
        CopyTransformer("schema_links", "filtered_schema_links"),
        AddSchema(tables_path),
        CopyTransformer("full_schema", "schema"),
        AddSymbolTable(tables_path),
        LinkDbId(),
        AddSymbolicQuestion(),
    ], [])
    await pipeline.run(data_path, force=True)
    return read_json_to_dict(os.path.join(cache_dir, "output.json"), "idx")
