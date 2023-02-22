""" Multiprocess consumer producer rabbit main. """
import logging
from multiprocessing import Event, Queue, Manager
from time import sleep

from consumer import RabbitConsumer
from producer import RabbitPublisher
from reader import Reader
from ulogger import LOG_FORMAT
from writer import Writer

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """ Main """
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    LOGGER.info("Starting main ...")
    shutdown_event = Event()
    manager = Manager()

    _map = manager.dict()

    reader_q = Queue()
    writer_q = Queue()

    reader = Reader(job_q=reader_q)
    writer = Writer(job_q=writer_q)

    publisher = RabbitPublisher(
        amqp_url='amqp://user:bitnami@localhost:5672?connection_attempts=3&heartbeat=3600',
        name="RabbitPublisherProc",
        shutdown_event=shutdown_event,
        job_q=reader_q,
        _map=_map
    )

    consumer = RabbitConsumer(
        amqp_url='amqp://user:bitnami@localhost:5672',
        shutdown_event=shutdown_event,
        job_q=writer_q
    )

    LOGGER.info("???? Starting reader ...")
    reader.start()

    LOGGER.info("???? Starting writer ...")
    writer.start()

    LOGGER.info("???? Starting publisher ...")
    publisher.start()

    LOGGER.info("???? Starting consumer ...")
    consumer.start()

    try:
        while True:
            LOGGER.info(".\n\n\n\n")
            sleep(1)
    except KeyboardInterrupt:
        pass

    LOGGER.info("!!!! Stopping reader. ")
    reader.stop()

    LOGGER.info("!!!! Joining reader. ")
    reader.join()

    LOGGER.info("!!!! Stopping writer. ")
    writer.stop()

    LOGGER.info("!!!! Joining writer. ")
    writer.join()

    LOGGER.info("!!!! Stopping reader. ")
    reader.stop()

    LOGGER.info("!!!! Stopping Child procs. ")
    shutdown_event.set()

    LOGGER.info("!!!! Stopping Manager.")
    manager.shutdown()


if __name__ == '__main__':
    main()
