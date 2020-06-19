import logging
import threading

from ..core.config import FILE_SYSTEMS

logging.root.setLevel(level=logging.INFO)


class Loader:
    def __init__(self):
        self._in_progress = True
        self._name = "Loader"
        self._logger = logging.getLogger(self._name)
        self._fs = FILE_SYSTEMS["aws_s3"]
        self._daemon = True

    def start(self):
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = self._daemon  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        self._logger.warning("NOT IMPLEMENTED")
        pass
