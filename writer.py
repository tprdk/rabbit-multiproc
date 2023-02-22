import logging
import multiprocessing
from queue import Empty
from threading import Thread

LOGGER = logging.getLogger(__name__)


class Writer(Thread):
    def __init__(self,
                 job_q: multiprocessing.Queue):
        super().__init__(name="Writer-Thread")
        self.is_interrupted = False
        self.job_q = job_q

    def stop(self):
        self.is_interrupted = True

    def write(self, message: str):
        LOGGER.info("Writing message.")
        with open("out.txt", "a") as fp:
            fp.write(f"{message}\n")

    def run(self) -> None:
        """ Mainloop writer thread """
        i = 0
        while not self.is_interrupted:
            try:
                msg = self.job_q.get(timeout=.1)
                self.write(msg)
            except Empty:
                continue
            except KeyboardInterrupt as err:
                self.stop()
