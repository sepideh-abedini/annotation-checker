from src.json_utils import read_json, write_json

in_path = "data/bird/questions.orig.json"

data = read_json(in_path)
data = sorted(data, key=lambda r: len(r['question']))

write_json(in_path, data)
