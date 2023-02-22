import logging
import multiprocessing
import time
from threading import Thread

LOGGER = logging.getLogger(__name__)


class Reader(Thread):
    def __init__(self,
                 job_q: multiprocessing.Queue):
        super().__init__(name="Reader-Thread")
        self.is_interrupted = False
        self.job_q = job_q

    def stop(self):
        self.is_interrupted = True

    def run(self) -> None:
        """ Mainloop writer thread """
        i = 0
        while not self.is_interrupted:
            try:
                LOGGER.info("Reader adding new message.")
                self.job_q.put(f"message : {i}")
                time.sleep(.5)
            except KeyboardInterrupt as err:
                self.stop()
            finally:
                i += 1
