import csv
import os
import tempfile
import heapq
import functools

import concurrent.futures

from async_component import AsyncComponent

from utils import read_file_line_by_line

class SortComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        raise NotImplementedError("Function {} not implemented".format("_read_input"))

    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["CHUNK_SIZE"] = config.get("CHUNK_SIZE", 3)

        return config

    # Sort files
    # key: this is a condition used to sort the lines, default not change any data
    # if need to take into account the header (not been sortered)
    def _sort_file(self, filepath, key=None, has_header=False):
        chunk_size = self._config["CHUNK_SIZE"]
        header = None
        chunk = []
        temp_files = []
        futures = []

        for line in read_file_line_by_line(filepath):
            if has_header and not header:
                header = line
            else:
                chunk.append(line)

            # Write by chunks
            if len(chunk) >= chunk_size:
                futures.append(
                    self._executor.submit(
                        self._write_sorted_temp_file, 
                        self._TMP_PATH, 
                        chunk,
                        key=key
                    )
                )
                chunk = []

        # Write the last chunk
        if len(chunk) >= 0:
            futures.append(
                self._executor.submit(
                    self._write_sorted_temp_file, 
                    self._TMP_PATH, 
                    chunk,
                    key=key
                )
            )

        for future in concurrent.futures.as_completed(futures):
            temp_files.append(future.result())

        # As only one writer, not concurrency on this part
        output_filepath = os.path.join(self._TMP_PATH, os.path.basename(os.path.normpath(filepath)))
        self._generate_sorted_file_by_temp_files(output_filepath, temp_files, key=key)
        return output_filepath

    @classmethod
    def _write_sorted_temp_file(cls, temp_directory, data, key=None):
        key = key if key else lambda line : line

        with tempfile.NamedTemporaryFile(mode="w", dir=temp_directory, delete=False) as temp_file:
            for line in sorted(data, key=key):
                temp_file.write("{}\n".format(line))
            return temp_file.name

    @classmethod
    def _generate_sorted_file_by_temp_files(cls, output_filepath, temp_files, key=None):
        key = key if key else lambda line : line

        with open(output_filepath, "w") as fw:
            file_handles = [ open(temp_file, "r") for temp_file in temp_files ]

            queue = []

            for idx, fr in enumerate(file_handles):
                line = fr.readline().strip()
                if line:
                    heapq.heappush(queue, (key(line), idx, line))

            while len(queue) > 0:
                _, idx, line = heapq.heappop(queue)
                fw.write("{}\n".format(line))

                line = file_handles[idx].readline().strip()
                if line:
                    heapq.heappush(queue, (key(line), idx, line))

            for fr in file_handles:
                fr.close()

        return output_filepath
