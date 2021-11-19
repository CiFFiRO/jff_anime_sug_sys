import yaml
import os
from typing import Dict, Optional
import logging
import sys
import dataclasses

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
import xml.etree.ElementTree as ET


LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass()
class ProcessingInfo:
    """Class for store info."""
    sql_context: SQLContext
    hive_context: HiveContext
    config: Dict

    def table_name(self) -> str:
        """Return full work table name.

        :return:
        """
        return f'`{self.config["hive"]["database"]}`.`{self.config["hive"]["mal_table"]}`'


def build_precessing_info() -> Optional[ProcessingInfo]:
    """Setup all requirements and aggregate it into info object.

    :return:
    """
    master = os.environ['MASTER']

    with open('hive/config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.BaseLoader)

    spark_config = SparkConf().setAppName("myFirstApp").setMaster(master)
    spark_config.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
    spark_context = SparkContext(conf=spark_config)
    sql_context = SQLContext(spark_context)
    hive_context = HiveContext(spark_context)

    return ProcessingInfo(
        sql_context=sql_context, hive_context=hive_context, config=config
    )


def update_mal(info: ProcessingInfo) -> None:
    """

    :param info:
    :return:
    """
    hive_context, config = info.hive_context, info.config
    create_script = f'''
        create table if not exists {info.table_name()} (
            `anime_id` bigint,
            `score` bigint
        );
    '''

    query = [f'insert overwrite table {info.table_name()} values ']
    root_node = ET.parse(config['other']['mal_file']).getroot()
    for tag in root_node.findall('anime'):
        anime_id, score, status = None, None, None
        for subtag in tag:
            if subtag.tag == 'series_animedb_id':
                anime_id = subtag.text
            if subtag.tag == 'my_score':
                score = subtag.text
            if subtag.tag == 'my_status':
                status = subtag.text

        if type(None).__name__ not in (type(x).__name__ for x in (anime_id, score, status)) and status == 'Completed':
            query.append(f'({anime_id}, {score})')
            query.append(',')

    query.pop()
    query.append(';')

    hive_context.sql(create_script)
    hive_context.sql('\n'.join(query))


if __name__ == '__main__':
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout
    )
    LOGGER.setLevel(logging.INFO)

    info = build_precessing_info()
    assert info is not None

    update_mal(info)
