import os
import multiprocessing
import logging
import json
from typing import List, Dict, Tuple

import yaml
from pyhive import hive, exc
from confluent_kafka import Consumer


def flush_data(
        data: List[Tuple[str, int, int, int]], logger: logging.Logger,
        cursor: hive.Cursor, config
) -> None:
    """Insert data into Hive table.

    :param data: list content.
    :param logger: logger.
    :param cursor: hive cursor.
    :param config: configuration.
    :return:
    """
    if len(data) == 0:
        return

    query = [f"insert into table {config['hive']['database']}.{config['hive']['table']} values"]
    for row in data:
        query.append(f"('{row[0]}', {row[1]}, {row[2]}, {row[3]})")
        query.append(',')
    query.pop()
    query = ' '.join(query)

    try:
        cursor.execute(query)
    except exc.Error as error:
        logger.error(f'Insert in table ending error: {error}')

    data.clear()


def loader(secrets: Dict[str, str], config) -> None:
    """Load data to Hive.

    :param secrets: secret info.
    :param config: config dictionary.
    :return:
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        connection = hive.Connection(
            host=secrets['hive_ip'], port=int(config['hive']['port']),
            username=config['hive']['user'], database=config['hive']['database'],
            configuration=config['hive']['settings']
        )
        cursor = connection.cursor()
    except BaseException as message:
        logger.error(f'Get error while create cursor {message}')
        return

    topic = config['consumer']['topic']
    group = config['consumer']['group']
    consumer = Consumer({
        'bootstrap.servers': secrets['kafka_broker'],
        'group.id': group,
        'auto.offset.reset': 'earliest'
    })

    data = []
    limit_data_length = int(config['consumer']['insert_batch_size'])
    consumer.subscribe([topic])
    while True:
        message = consumer.poll(15.0)
        if message is None:
            logger.error('Timeout exceeded.')
            break
        if message.error() is not None:
            logger.warning(f'Get message error {message.error().code()}')
            continue

        row = json.loads(message.value().decode('utf8'))
        data.append((row['user_name'], row['anime_id'], row['score'], row['status']))
        if len(data) > limit_data_length:
            flush_data(data, logger, cursor, config)

    flush_data(data, logger, cursor, config)
    consumer.close()


if __name__ == '__main__':
    secrets = {}
    for variable in ('kafka_broker', 'hive_password', 'hive_ip'):
        secrets.update({variable: os.environ[variable.upper()]})

    with open('kafka/config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.BaseLoader)

    processes = [
        multiprocessing.Process(target=loader, args=(secrets, config))
        for i in range(multiprocessing.cpu_count())
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join()
