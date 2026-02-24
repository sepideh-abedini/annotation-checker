import asyncio

from src.a1_extract_alt_dbs import extract_alt_dbs
from src.a2_orig_mask import mask_questions
from src.json_utils import write_json, read_json, read_json_to_dict
from src.pipe.schema_repo import DatabaseSchemaRepo


def extract_alts():
    data = read_json("data/questions.json")
    schema_repo = DatabaseSchemaRepo("data/tables.json")
    symb_data = read_json_to_dict("data/12_AddSymbolicQuestion.json", "idx")
    new_data = []
    for row in data:

        if "alt_question" in row:
            question = row['alt_question']
            db_id = row['alt_db_id']
            schema = schema_repo.dbs[db_id].to_yaml()
        else:
            question = None
            db_id = None
            schema = None
        idx = row['idx']
        symb_row = symb_data[idx]
        symb_question = symb_row['symbolic']['question']

        new_data.append({
            "idx": idx,
            "question": question,
            "db_id": db_id,
            "schema": schema,
            "original_symbolic": symb_question,
            "original_question": row['question'],
            "original_db_id": row['db_id'],
        })
    write_json("alt_questions.json", new_data)


if __name__ == '__main__':
    extract_alt_dbs()
    asyncio.run(mask_questions())
    # extract_alts()
