import asyncio
import os

import yaml

from src.a1_extract_alt_dbs import extract_alt_dbs
from src.a2_orig_mask import mask_questions
from src.a3_extract_alt import extract_alts
from src.json_utils import write_json, read_json
from src.pipe.schema_repo import DatabaseSchemaRepo


def extract_dbs():
    schema_repo = DatabaseSchemaRepo("data/tables.json")
    all_schemas = read_json("data/tables.json")
    questions = read_json("alt_questions.json")
    used_dbs = set(map(lambda r: r['db_id'], questions))
    os.makedirs("schemas", exist_ok=True)
    alt_schemas = dict()
    for db_id, schema in schema_repo.dbs.items():
        if db_id in used_dbs:
            alt_schemas[db_id] = schema.tables
            with open(f"schemas/{db_id}.yaml", "w") as f:
                f.write(schema.to_yaml())
    used_schemas = []
    for schema in all_schemas:
        db_id = schema['db_id']
        if db_id in used_dbs:
            used_schemas.append(schema)
    write_json("alt_schemas.json", used_schemas)

    with open("alt_schemas.yaml", "w") as f:
        f.write(yaml.dump(alt_schemas))


if __name__ == '__main__':
    extract_alt_dbs()
    asyncio.run(mask_questions())
    extract_alts()
    extract_dbs()
