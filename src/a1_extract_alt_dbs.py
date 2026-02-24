from src.json_utils import read_json, write_json


def extract_alt_dbs():
    all_schemas = read_json("tables.all.json")
    questions = read_json("questions.json")
    used_dbs = set(map(lambda r: r['alt_db_id'], questions))
    used_schemas = []
    for schema in all_schemas:
        db_id = schema['db_id']
        if db_id in used_dbs:
            used_schemas.append(schema)
    write_json("alt_schemas.json", used_schemas)


if __name__ == '__main__':
    extract_alt_dbs()
