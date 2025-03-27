from component import Component
from concurrent.futures import ProcessPoolExecutor
        
class AsyncComponent(Component):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        raise NotImplementedError("Function {} not implemented".format("_read_input"))

    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["WORKERS"] = config.get("WORKERS", 10)

        return config

    def process(self):
        super().process()
        self._executor = ProcessPoolExecutor(max_workers=self._config["WORKERS"])

    def __del__(self):
        self._executor.shutdown(wait=True)