import re
from typing import Dict

from src.pipe.processor.list_transformer import JsonListTransformer


def split_to_words(s):
    return [w.lower() for w in re.split(r'[^a-zA-Z0-9]+', s) if w]


def get_n_grams(s, lower=1, upper=5):
    words = s.split()
    ngrams = [' '.join(words[i:i + n]) for n in range(lower, upper + 1) for i in range(len(words) - n + 1)]
    return ngrams


class LinkDbId(JsonListTransformer):
    async def _process_row(self, row: Dict) -> Dict:
        # match world_1
        db_id = row['db_id']
        question = row['question']
        schema_links = row['schema_links']
        value_links = row['value_links']

        refs = []

        db_id_words = split_to_words(db_id)
        for ng in get_n_grams(question):
            ng_words = split_to_words(ng)
            if ng_words == db_id_words:
                refs.append(ng)

        for ref in refs:
            for key in list(value_links.keys()):
                if ref.lower() == key.lower():
                    if key in value_links:
                        del value_links[key]
                if db_id_words == split_to_words(key):
                    if key in value_links:
                        del value_links[key]

        # for ref in refs:
        #     for key in list(schema_links.keys()):
        #         if ref.lower() == key.lower():
        #             if key in schema_links:
        #                 del schema_links[key]
        #         if db_id_words == split_to_words(key):
        #             if key in schema_links:
        #                 del schema_links[key]
        #
        row['db_id_refs'] = refs
        row['schema_links'] = schema_links
        row['value_links'] = value_links
        return row
