import json
from abc import ABC, abstractmethod
from dataclasses import dataclass


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"Failed to read file: {path}") from e


def read_json_to_dict(path, key_field):
    data = read_json(path)
    return {item[key_field]: item for item in data}


def write_json(path, data):
    with open(path, 'w') as f:
        f.write(json.dumps(data, indent=4))

def write_yaml(path, data):
    with open(path, "w") as f:
        f.write(data)

@dataclass
class JsonIterator(ABC):
    in_path: str

    def __init__(self, in_path: str):
        self.in_path = in_path

    @abstractmethod
    def proc_row(self, row, i):
        pass

    def run(self):
        data = read_json(self.in_path)
        for i, row in enumerate(data):
            self.proc_row(row, i)


@dataclass
class JsonProcessor(JsonIterator):
    in_path: str
    out_path: str

    def __init__(self, in_path: str, out_path):
        self.in_path = in_path
        self.out_path = out_path

    @abstractmethod
    def proc_row(self, row, i):
        pass

    def run(self):
        data = read_json(self.in_path)

        updated_rows = []
        for i, row in enumerate(data):
            updated_row = self.proc_row(row, i)
            updated_rows.append(updated_row)

        with open(self.out_path, 'w') as f:
            f.write(json.dumps(updated_rows, indent=4))


def copy_prop(source, dst, prop, key):
    source_data = read_json_to_dict(source, key)
    dst_data = read_json_to_dict(dst, key)
    assert set(source_data.keys()) == set(dst_data.keys())
    for k, v in source_data.items():
        dst_data[k][prop] = v[prop]

    write_json(dst, list(dst_data.values()))
