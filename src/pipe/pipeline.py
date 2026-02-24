import os
import shutil
from typing import List

from loguru import logger

from src.pipe.monitor.lib import Timer
from src.pipe.processor.list_transformer import JsonListTransformer
from src.pipe.processor.report_step import ReportStep
from src.util.file_utils import save_hash, verify_file_hash

HALT_AT_STEP = os.environ.get("HALT_AT_STEP", None)


class Pipeline:
    __initial_file: str = "1_input.json"
    __output_file: str = "output.json"
    __output_hash: str = "output.hash"
    report_steps: List[ReportStep]

    def __init__(self, stages: List[JsonListTransformer], report_steps: List[ReportStep]):
        self.stages = stages
        self.report_steps = report_steps

    async def __run_internal(self, input_file):
        tmp_file = input_file
        timer = Timer()
        timer.start()
        for stage in self.stages:
            logger.debug(f"Starting Stage: {stage.name}")
            tmp_file, _ = await stage.run(tmp_file)
            logger.debug(f"Done Stage: {stage.name}, time={timer.lap()}")
        return tmp_file

    async def final_report(self, output_file):
        result = {}
        for step in self.report_steps:
            await step.run(output_file)
            step_result = step.get_result()
            result = result | step_result
        return result

    def get_init_file(self, input_file):
        dir_path = os.path.dirname(input_file)
        init_file_path = os.path.join(dir_path, self.__initial_file)
        shutil.copyfile(input_file, init_file_path)
        return init_file_path

    def save_output_file(self, dir_path, out_file):
        output_path = os.path.join(dir_path, self.__output_file)
        hash_path = os.path.join(dir_path, self.__output_hash)
        shutil.copyfile(out_file, output_path)
        save_hash(output_path, hash_path)

    def hash_is_valid(self, dir_path):
        output_path = os.path.join(dir_path, self.__output_file)
        hash_path = os.path.join(dir_path, self.__output_hash)
        return verify_file_hash(output_path, hash_path)

    async def run(self, input_file, force: bool = False):
        dir_path = os.path.dirname(input_file)
        if self.hash_is_valid(dir_path) and not force:
            logger.info("Output file hash is valid, skipping pipeline.")
            out_file = os.path.join(dir_path, self.__output_file)
        else:
            init_file = self.get_init_file(input_file)
            out_file = await self.__run_internal(init_file)
            self.save_output_file(dir_path, out_file)
        run_result = await self.final_report(out_file)
        return run_result
