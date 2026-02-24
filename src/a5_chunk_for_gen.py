import asyncio
import json
import os

from src.a1_extract_alt_dbs import extract_alt_dbs
from src.a2_orig_mask import mask_questions
from src.a3_extract_alt import extract_alts
from src.a4_extract_dbs import extract_dbs
from src.json_utils import read_json

CHUNK_SIZE = 10


def chunk_for_anon():
    data = read_json("alt_questions.json")

    os.makedirs("chunks", exist_ok=True)

    for i in range(0, len(data), CHUNK_SIZE):
        chunk = data[i:i + CHUNK_SIZE]
        for row in chunk:
            row['gold_schema_links'] = "???"
            row['gold_value_links'] = "???"
            del row['original_question']
            del row['original_db_id']
            del row['original_symbolic']
        with open(f"chunks/alt_questions_{i // CHUNK_SIZE}.json", "w") as f:
            json.dump(chunk, f, indent=2)


if __name__ == '__main__':
    extract_alt_dbs()
    asyncio.run(mask_questions())
    extract_alts()
    extract_dbs()
    chunk_for_anon()
