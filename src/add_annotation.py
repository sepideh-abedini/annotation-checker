from src.json_utils import read_json, write_json

chunk = 11

chunk_path = f"data/bird/chunks/chunk_{chunk}.json"
chunk_data = read_json(chunk_path)
anon = read_json(f"data/bird/annotated/chunk_{chunk}.json")

keys = ["db_id", "question", "evidence", "gold_schema_links", "gold_value_links", "gold_sql"]

for i, row in enumerate(chunk_data):
    a = anon[i]
    for k in keys:
        row[k] = a[k]

write_json(chunk_path, chunk_data)
