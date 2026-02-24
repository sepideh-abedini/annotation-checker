from typing import Dict, Union, List, Tuple

from loguru import logger

from src.pipe.processor.list_transformer import JsonListTransformer
from src.pipe.utils import replace_str_punc


class AddSymbolicQuestion(JsonListTransformer):
    def __init__(self):
        super().__init__(force=True)
        # self.su = StringUtils()
        self.su = None

    def get_symbol(self, schema_items: Union[List[str], str], symbol_table: Dict[str, str]) -> str:
        if not isinstance(schema_items, list):
            schema_items = [schema_items]
        symbols = []
        for schema_item in schema_items:
            schema_item_parts = schema_item.split(":")
            schema_item = schema_item_parts[1]
            symbol = symbol_table.get(schema_item)
            symbols.append(symbol)
        return ",".join(symbols)

    def symbolize_term(self, question: str, question_term: str, schema_items: str,
                       symbol_table: Dict[str, str], lemma: bool) -> str:
        symbol = self.get_symbol(schema_items, symbol_table)
        if symbol is None:
            logger.error(f"Symbol is none for {schema_items}")
        if lemma:
            symbolic_question = self.su.replace_str_lemma(question, question_term, symbol)
        else:
            symbolic_question = replace_str_punc(question, question_term, symbol)
        return symbolic_question, symbol

    def symbolize_value(self, question: str, question_term: str, col_refs: str | List[str],
                        updated_schema_links: Dict[str, str],
                        filtered_value_links: Dict[str, str],
                        symbol_table: Dict[str, str], lemma: bool) -> Tuple[str, str]:
        value_symbol = f"[V{self.vid}]"
        if not isinstance(col_refs, list):
            col_refs = [col_refs]
        column_symbols = []
        for col_ref in col_refs:
            if col_ref in filtered_value_links.values() or f"COLUMN:{col_ref}" in updated_schema_links.values():
                column_symbol = symbol_table[col_ref]
            else:
                column_symbol = col_ref
            column_symbols.append(column_symbol)
        column_symbol = ",".join(column_symbols)
        self.vid += 1
        evidence = f"{value_symbol} is a value of the column {column_symbol}"
        self.value_dict[value_symbol] = question_term
        if lemma:
            symbolic_question = self.su.replace_str_lemma(question, question_term, value_symbol)
        else:
            symbolic_question = replace_str_punc(question, question_term, value_symbol)
        if question != symbolic_question:
            symbolic_question = f"{symbolic_question}; {evidence}"
        return symbolic_question, value_symbol

    def add_tables_of_columns(self, schema_links: Dict[str, str], filtered_schema_links: Dict[str, str]):
        updated_schema_links = filtered_schema_links.copy()
        tables = set()
        for schema_items in filtered_schema_links.values():
            if schema_items is None:
                logger.error(f"Invalid schema item: {schema_items}")
                continue
            if not isinstance(schema_items, list):
                schema_items = [schema_items]
            for schema_item in schema_items:
                if schema_item.startswith("COLUMN"):
                    col_ref = schema_item.split(":")[1]
                    table_name = col_ref.split(".")[0]
                    tables.add(table_name)

        for question_term, schema_items in schema_links.items():
            if not isinstance(schema_items, list):
                schema_items = [schema_items]
            for schema_item in schema_items:
                if schema_item.startswith("TABLE"):
                    assert len(schema_items) == 1
                    table_name = schema_item.split(":")[1]
                    if table_name in tables:
                        updated_schema_links[question_term] = schema_item
        return updated_schema_links

    async def _process_row(self, row):
        if "symbolic" not in row:
            return row
        self.vid = 1
        self.value_dict = dict()
        filtered_schema_links = row['filtered_schema_links']
        schema_links = row['schema_links']
        db_id_refs = row['db_id_refs']
        question = row['question']
        symbol_table = row['symbolic']['to_symbol']
        updated_schema_links = self.add_tables_of_columns(schema_links, filtered_schema_links)
        masked_terms = dict()
        masked_to_link = dict()

        symbolic_question = question
        masked = 0

        value_links = row['value_links']
        filtered_value_links = row['filtered_value_links']

        if isinstance(value_links, list) or isinstance(value_links, str):
            logger.error(f"Invalid value links: {value_links}")
            value_links = dict()

        if isinstance(filtered_value_links, list) or isinstance(filtered_value_links, str):
            logger.error(f"Invalid value links: {filtered_value_links}")
            filtered_value_links = dict()

        # sorted_value_terms = sorted(value_links.keys(), key=len, reverse=True)
        sorted_value_terms = sorted(filtered_value_links.keys(), key=len, reverse=True)
        sorted_schema_terms = sorted(updated_schema_links.keys(), key=len, reverse=True)

        for question_term in sorted_value_terms:
            schema_items = filtered_value_links[question_term]
            try:
                prev_q = symbolic_question
                symbolic_question, symbol = self.symbolize_value(symbolic_question, question_term, schema_items,
                                                                 updated_schema_links,
                                                                 filtered_value_links, symbol_table, lemma=False)
                if prev_q != symbolic_question:
                    masked_terms[question_term] = symbol
                    masked_to_link[question_term] = schema_items
                    masked += 1
            except Exception as e:
                logger.error(f"Failed to mask {question_term}:{schema_items}, error={e} ")

        # for question_term in sorted_value_terms:
        #     schema_item = filtered_value_links[question_term]
        #     try:
        #         symbolic_question, symbol = self.symbolize_value(symbolic_question, question_term, schema_item,
        #                                                          updated_schema_links,
        #                                                          filtered_value_links, symbol_table, lemma=True)
        #         masked_terms[question_term] = symbol
        #         masked += 1
        #     except Exception as e:
        #         logger.error(f"Failed to mask {question_term}:{schema_item}, error={e} ")

        for question_term in sorted_schema_terms:
            schema_items = updated_schema_links[question_term]
            try:
                prev_q = symbolic_question
                symbolic_question, symbol = self.symbolize_term(symbolic_question, question_term, schema_items,
                                                                symbol_table, lemma=False)
                if prev_q != symbolic_question:
                    masked_terms[question_term] = symbol
                    if not isinstance(schema_items, list):
                        schema_items = [schema_items]
                    schema_refs = [v.split(":")[1] for v in schema_items]
                    masked_to_link[question_term] = schema_refs if len(schema_refs) > 1 else schema_refs[0]
                    masked += 1
            except Exception as e:
                logger.error(f"Failed to mask {question_term}:{schema_items}, error={e} ")

        symbol = "[DB_NAME]"
        for question_term in db_id_refs:
            try:
                prev_q = symbolic_question
                symbolic_question = replace_str_punc(symbolic_question, question_term, symbol)
                if prev_q != symbolic_question:
                    masked_terms[question_term] = symbol
                    masked += 1
            except Exception as e:
                logger.error(f"Failed to mask {question_term}:{symbol}, error={e} ")

        # for question_term in sorted_schema_terms:
        #     schema_items = updated_schema_links[question_term]
        #     try:
        #         symbolic_question, symbol = self.symbolize_term(symbolic_question, question_term, schema_items,
        #                                                         symbol_table, lemma=True)
        #         masked_terms[question_term] = symbol
        #         masked += 1
        #     except Exception as e:
        #         logger.error(f"Failed to mask {question_term}:{schema_items}, error={e} ")

        row['symbolic'].update(
            {
                "question": symbolic_question,
                "to_value": self.value_dict,
                "masked": masked,
                "masked_terms": masked_terms,
                "masked_to_links": masked_to_link
            }
        )
        return row
