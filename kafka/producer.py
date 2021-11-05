import os
import csv
import sys
import json
import logging
import multiprocessing
from uuid import uuid4

import yaml
from confluent_kafka import Producer


def loader(broker: str, cpu_number: int, trigger_number: int, config: dict) -> None:
    """Kafka producer part csv data.

    :param broker: broker address.
    :param cpu_number: number cores.
    :param trigger_number: number of row by module cpu_number
    :param config: loaded config.
    :return:
    """
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    topic = config['producer']['topic']
    limit_batch_size = int(config['producer']['max_messages_per_batch'])
    producer = Producer({
        'bootstrap.servers': broker,
        'batch.num.messages': limit_batch_size,
        'compression.type': config['producer']['compression']
    })

    def delivery_report(error, _) -> None:
        if error is not None:
            logger.error('Message delivery failed: {}'.format(error))

    with open(config['producer']['data_path'], 'r', newline='') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        index = 1
        batch_size = 0
        for row in csv_reader:
            if index % cpu_number == trigger_number:
                try:
                    data = json.dumps({
                        'user_name': str(row[0]), 'anime_id': int(row[1]),
                        'score': int(row[5]), 'status': int(row[6])
                    })
                    producer.produce(
                        topic=topic, value=data.encode('utf8'),
                        key=str(uuid4()), callback=delivery_report
                    )
                    batch_size += 1
                    if batch_size == limit_batch_size - 1:
                        producer.flush()
                        batch_size = 0
                except KeyError:
                    logger.info(f'String with bad format: {row}')
                except ValueError:
                    logger.info(f'String with bad data: {row}')
            index += 1
        producer.flush()


if __name__ == '__main__':
    broker = os.environ['KAFKA_BROKER']

    with open('kafka/config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.BaseLoader)

    processes = [
        multiprocessing.Process(target=loader, args=(broker, multiprocessing.cpu_count(), i, config))
        for i in range(multiprocessing.cpu_count())
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join()
