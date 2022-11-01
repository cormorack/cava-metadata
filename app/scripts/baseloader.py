from loguru import logger
import threading
import fsspec

from core.config import FILE_SYSTEMS


class Loader:
    def __init__(self):
        self._in_progress = True
        self._name = "Loader"
        self._logger = logger
        self._fs_kwargs = FILE_SYSTEMS["aws_s3"]
        self._daemon = True
        self._fs = fsspec.filesystem(**self._fs_kwargs)

    def start(self):
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = self._daemon  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        self._logger.warning("NOT IMPLEMENTED")
        pass
