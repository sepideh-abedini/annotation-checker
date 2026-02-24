from itertools import combinations
from typing import List

from src.pipe.processor.list_transformer import JsonListTransformer
from src.pipe.schema_repo import DatabaseSchemaRepo, DatabaseSchema
import networkx as nx
from loguru import logger


def get_join_tables(schema: DatabaseSchema, tables: set[str]):
    # We are finding the list of connecting tables here
    G = nx.Graph()
    for table_name, table_columns in schema.tables.items():
        G.add_node(table_name)
        for col_name, col_data in table_columns.items():
            if isinstance(col_data, dict) and "foreign_key" in col_data:
                fk_ref = col_data["foreign_key"]
                dst_table = fk_ref.split(".")[0]
                G.add_edge(table_name, dst_table)
    join_tables = set()
    for a, b in combinations(tables, 2):
        for path in list(nx.all_simple_paths(G, source=a, target=b)):
            for table in path:
                join_tables.add(table)
    return join_tables


def filter_schema(schema: DatabaseSchema, schema_items: List[str], include_fks: bool = True):
    columns = set()
    for item in schema_items:
        item_ref = item.split(":")[1]
        if "[*]" in item_ref:
            continue
        if item.split(":")[0] == "COLUMN":
            columns.add(item_ref)

    main_tables = set()
    for item in schema_items:
        if "TABLE:" in item:
            main_tables.add(item.split("TABLE:")[1])
    for col_ref in list(columns):
        table_name = col_ref.split(".")[0]
        main_tables.add(table_name)
        col_name = col_ref.split(".")[1]
        col_data = schema.tables[table_name][col_name]
        if isinstance(col_data, dict) and "foreign_key" in col_data:
            fk_ref = col_data["foreign_key"]
            columns.add(fk_ref)

    join_tables = get_join_tables(schema, main_tables)

    filtered_schema = DatabaseSchema()
    all_tables = main_tables.union(join_tables)
    for table in all_tables:
        filtered_schema.tables[table] = dict()

    for table_name, table_columns in schema.tables.items():
        if table_name in filtered_schema.tables:
            filtered_table_columns = filtered_schema.tables[table_name]
        else:
            filtered_table_columns = dict()
        for col_name, col_data in table_columns.items():
            if f"{table_name}.{col_name}" in columns:
                if not include_fks and isinstance(col_data, dict) and (
                        "primary_key" in col_data or "foreign_key" in col_data):
                    continue
                filtered_table_columns[col_name] = col_data
            if isinstance(col_data, dict) and "primary_key" in col_data and (table_name in main_tables):
                filtered_table_columns[col_name] = col_data
            if isinstance(col_data, dict) and "foreign_key" in col_data:
                ref = col_data['foreign_key']
                ref_table = ref.split(".")[0]
                ref_col = ref.split(".")[1]
                if table_name in all_tables and ref_table in all_tables:
                    filtered_table_columns[col_name] = col_data
                    filtered_schema.tables[ref_table][ref_col] = schema.tables[ref_table][ref_col]
        if len(filtered_table_columns) > 0:
            filtered_schema.tables[table_name] = filtered_table_columns

    # for table_name, table_columns in filtered_schema.tables.items():
    #     for col_name, col_data in table_columns.items():
    #         if isinstance(col_data, dict) and "foreign_key" in col_data:
    #             ref = col_data['foreign_key']
    #             ref_table = ref.split(".")[0]

    return filtered_schema


class AddSchema(JsonListTransformer):

    def __init__(self, tables_path):
        super().__init__(force=True)
        self.schema_repo = DatabaseSchemaRepo(tables_path)

    async def _process_row(self, row):
        if row['db_id'] in self.schema_repo.dbs:
            schema = self.schema_repo.dbs[row['db_id']].to_yaml()
        else:
            schema = None
            logger.warning(f"Schema not found: {row['db_id']}")
        row['full_schema'] = schema
        return row


class AddFilteredSchema(JsonListTransformer):

    def __init__(self, tables_path):
        super().__init__(force=True)
        self.schema_repo = DatabaseSchemaRepo(tables_path)

    async def _process_row(self, row):
        schema = self.schema_repo.dbs[row['db_id']]
        schema_items = row['schema_items']
        filtered_schema = filter_schema(schema, schema_items)
        row['schema'] = filtered_schema.to_yaml()
        return row


class AddNoFkSchema(JsonListTransformer):

    def __init__(self, tables_path):
        super().__init__(force=True)
        self.schema_repo = DatabaseSchemaRepo(tables_path)

    async def _process_row(self, row):
        schema = self.schema_repo.dbs[row['db_id']]
        schema_items = row['schema_items']
        filtered_schema = filter_schema(schema, schema_items, False)
        row['schema_no_fk'] = filtered_schema.to_yaml()
        return row


class AddSchemaItems(JsonListTransformer):
    def __init__(self, tables_path):
        super().__init__(force=True)
        self.schema_repo = DatabaseSchemaRepo(tables_path)

    async def _process_row(self, row):
        schema = self.schema_repo.dbs[row['db_id']]
        schema_items = []
        for table, columns in schema.tables.items():
            schema_items.append(f"TABLE:{table}")
            for col, col_data in columns.items():
                schema_items.append(f"COLUMN:{table}.{col}")
            schema_items.append(f"COLUMN:{table}.[*]")
        row['schema_items'] = schema_items
        return row
