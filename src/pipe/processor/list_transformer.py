import json
import os
from abc import ABC

from loguru import logger

from src.json_utils import read_json
from src.pipe.processor.list_processor import JsonListProcessor

FORCE = int(os.environ.get("FORCE", 0)) > 0

PROPAGATE_PROPS = {
    "gold_schema_links",
    "gold_value_links",
    # "perfect_recall",
    "perfect_recalls"
}



class JsonListTransformer(JsonListProcessor, ABC):
    def __init__(self, force=True):
        self.force = force or FORCE
        self.logger = logger.bind(type="report", name=self.name)

    def get_output_file(self, input_file):
        file_name = os.path.basename(input_file)
        dir_path = os.path.dirname(input_file)
        num = int(file_name.split("_", 1)[0])
        return os.path.join(dir_path, f"{num + 1}_{self.name}.json")

    async def run(self, input_file):

        output_file = self.get_output_file(input_file)

        logger.info(f"Looking for file: {output_file}.")
        if not self.force and os.path.exists(output_file):
            logger.info(f"File exists: {output_file}, skipping.")
            in_data = self._get_input_data(input_file)
            out_rows = read_json(output_file)
            if len(in_data) == len(out_rows):
                assert len(in_data) == len(out_rows)
                for i, in_row in enumerate(in_data):
                    out_row = out_rows[i]
                    for k in PROPAGATE_PROPS:
                        if k in in_row:
                            out_row[k] = in_row[k]
                if len(PROPAGATE_PROPS) > 0:
                    with open(output_file, "w") as f:
                        f.write(json.dumps(out_rows, indent=4))
            return output_file, out_rows

        updated_rows = await super().run(input_file)

        # updated_rows = []
        # for i, row in enumerate(self._get_input_data(input_file)):
        #     if self.prop_name in row:
        #         prop = row[self.prop_name]
        #         if isinstance(prop, dict):
        #             row[self.prop_name].update(output[i])
        #         elif isinstance(prop, int):
        #             row[self.prop_name] += int(output[i])
        #         else:
        #             raise Exception(f"Invalid prop type being updated: {self.prop_name}, {type(prop)}")
        #     else:
        #         row[self.prop_name] = output[i]
        #     updated_rows.append(row)

        with open(output_file, "w") as f:
            f.write(json.dumps(updated_rows, indent=4))

        return output_file, updated_rows

    @property
    def name(self):
        return self.__class__.__name__

    def __repr__(self):
        return f"{self.name}"
