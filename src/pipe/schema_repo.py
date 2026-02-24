import json
from typing import Dict, Union, List

import yaml


def normalize(name: str) -> str:
    name = name.lower()
    name = f"[{name}]"
    return name


class DatabaseSchema:
    tables: Dict[str, Dict[str, Union[str, Dict[str, Union[str, bool]]]]]

    def __init__(self):
        self.tables = {}

    def to_yaml(self) -> str:
        return yaml.dump(self.tables)

    def to_sqlglot_schema(self):
        mapping = dict()
        for table_name, columns in self.tables.items():
            mapping[table_name] = {}
            for col_name, col_info in columns.items():
                col_type = col_info
                if isinstance(col_info, dict) and "type" in col_info:
                    col_type = col_info["type"]
                if col_type == "number":
                    sqlglot_type = "INT"
                else:
                    sqlglot_type = "VARCHAR"
                mapping[table_name][col_name] = sqlglot_type
        return mapping

    @staticmethod
    def from_yaml(yaml_str: str):
        tables = yaml.full_load(yaml_str)
        db_schema = DatabaseSchema()
        db_schema.tables = tables
        return db_schema

    @property
    def struct(self) -> 'DatabaseSchema':
        filtered_schema = DatabaseSchema()
        for table_name, table_columns in self.tables.items():
            filtered_table_columns = dict()
            for col_name, col_data in table_columns.items():
                if isinstance(col_data, dict):
                    if "primary_key" in col_data or "foreign_key" in col_data:
                        filtered_table_columns[col_name] = col_data
            filtered_schema.tables[table_name] = filtered_table_columns
        return filtered_schema

    @property
    def col_seq(self):
        return sorted(map(len, self.tables.values()))

    def __str__(self):
        return self.to_yaml()

    def __repr__(self):
        return str(self)


class DatabaseSchemaRepo:
    dbs: Dict[str, DatabaseSchema]

    def load_json(self, tables_json_path):
        with open(tables_json_path) as file:
            data = json.load(file)
            for db in data:
                schema = DatabaseSchema()
                for table in db['table_names_original']:
                    schema.tables[normalize(table)] = {}
                pks = []
                for pk in db['primary_keys']:
                    if isinstance(pk, int):
                        pks.append(pk)
                    if isinstance(pk, list):
                        pks.extend(pk)
                for i, col in enumerate(db['column_names_original']):
                    table_idx = col[0]
                    if table_idx >= 0:
                        table_name = normalize(db['table_names_original'][table_idx])
                        col_name = normalize(col[1])
                        column_type = db['column_types'][i]
                        if i in pks:
                            schema.tables[table_name][col_name] = {"type": column_type, "primary_key": True}
                        else:
                            schema.tables[table_name][col_name] = column_type
                for foreign_keys in db['foreign_keys']:
                    src_col_idx = foreign_keys[0]
                    src_table_idx, src_col_name = db['column_names_original'][src_col_idx]
                    src_col_name = normalize(src_col_name)
                    src_table_name = normalize(db['table_names_original'][src_table_idx])

                    dst_col_idx = foreign_keys[1]
                    dst_table_idx, dst_col_name = db['column_names_original'][dst_col_idx]
                    dst_col_name = normalize(dst_col_name)
                    dst_table_name = normalize(db['table_names_original'][dst_table_idx])

                    col_type = schema.tables[src_table_name][src_col_name]
                    fk_ref = f"{dst_table_name}.{dst_col_name}"
                    if isinstance(col_type, str):
                        schema.tables[src_table_name][src_col_name] = {
                            "type": col_type,
                            "foreign_key": fk_ref
                        }
                    else:
                        schema.tables[src_table_name][src_col_name]["foreign_key"] = fk_ref

                self.dbs[db['db_id']] = schema

    def __init__(self, tables_path: str):
        self.dbs = {}
        if tables_path.endswith(".json"):
            self.load_json(tables_path)
        else:
            raise NotImplementedError()
