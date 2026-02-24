import os

from src.json_utils import read_json, write_json

CHUNK_SIZE = 10

in_path = "cache/orig/11_AddSymbolicQuestion.json"
out_path = "data/bird/questions.alt.json"

data = read_json(in_path)

anon_data = []
for row in data:
    anon_row = dict()
    anon_row['idx'] = row['idx']
    anon_row['db_id'] = "<DB_ID>"
    anon_row['orig_db_id'] = row['db_id']
    anon_row['question'] = "<QUESTION>"
    anon_row['orig_question'] = row['question']
    anon_row['orig_masked'] = row['symbolic']['question']
    anon_row['evidence'] = "<EVIDENCE>"
    anon_row['orig_evidence'] = row['evidence']
    anon_row['gold_schema_links'] = "<GOLD_SCHEMA_LINKS>"
    anon_row['orig_gold_schema_links'] = row['gold_schema_links']
    anon_row['gold_value_links'] = "<GOLD_VALUE_LINKS>"
    anon_row['orig_gold_value_links'] = row['gold_value_links']
    anon_row['gold_sql'] = "<GOLD_SQL>"
    anon_row['orig_gold_sql'] = row['query']
    anon_row['evidence_added'] = True
    anon_data.append(anon_row)

anon_data = sorted(anon_data, key=lambda r: len(r['orig_question']))

chunks = [anon_data[i:i + 10] for i in range(0, len(anon_data), 10)]

chunks_path = os.path.join("data", "bird", "chunks")

os.makedirs(chunks_path)

write_json(out_path, anon_data)

for i, chunk in enumerate(chunks):
    write_json(os.path.join(chunks_path, f"chunk_{i}.json"), chunk)
