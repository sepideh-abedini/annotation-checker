import os

from src.a3_extract_alt import extract_alts
from src.a4_extract_dbs import extract_dbs
from src.a5_chunk_for_gen import chunk_for_anon
from src.json_utils import write_json, read_json_to_dict


def add_anons(chunk):
    orig_data = read_json_to_dict("alt_questions.json", "idx")
    orig_chunk = read_json_to_dict(f"chunks/alt_questions_{chunk}.json", "idx")
    annon_chunk = read_json_to_dict(f"chunk_anons/alt_questions_{chunk}.json", "idx")

    os.makedirs("chunk_rev", exist_ok=True)

    chunk_rev = []

    for idx, row in orig_chunk.items():
        anon = annon_chunk[idx]
        orig = orig_data[idx]
        orig['gold_schema_links'] = anon['gold_schema_links']
        orig['gold_value_links'] = anon['gold_value_links']
        chunk_rev.append(orig)

    write_json(f"chunk_rev/alt_questions_{chunk}.json", chunk_rev)


if __name__ == '__main__':
    extract_alts()
    extract_dbs()
    chunk_for_anon()
    add_anons(0)
