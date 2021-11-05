import datetime

import pyspark.sql
import yaml
import os
from typing import Dict, Optional
import logging
import sys
import argparse
import dataclasses
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext


LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass()
class ProcessingInfo:
    """Class for store info."""
    spark_context: SparkContext
    sql_context: SQLContext
    hive_context: HiveContext
    config: Dict
    date: str

    def work_directory(self) -> str:
        """Return current work_directory.

        :return:
        """
        return f"{self.config['hadoop']['data_directory']}/{self.date}"


def build_precessing_info() -> Optional[ProcessingInfo]:
    """Setup all requirements and aggregate it into info object.

    :return:
    """
    master = os.environ['MASTER']

    command_line_arguments_parser = argparse.ArgumentParser()
    command_line_arguments_parser.add_argument('--date', type=str, help='Date in YY-MM-DD format (Partition key).')
    command_line_arguments = command_line_arguments_parser.parse_args()
    processing_date = command_line_arguments.date

    try:
        datetime.datetime.strptime(processing_date, '%y-%m-%d')
    except ValueError:
        LOGGER.error(f'Script executed with bad date format: {processing_date} instead of %y-%m-%d')
        return None

    with open('spark/config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.BaseLoader)

    spark_config = SparkConf().setAppName("myFirstApp").setMaster(master)
    spark_config.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    spark_config.set('hive.exec.dynamic.partition.mode', 'nonstrict')
    spark_context = SparkContext(conf=spark_config)
    sql_context = SQLContext(spark_context)
    hive_context = HiveContext(spark_context)

    return ProcessingInfo(
        spark_context=spark_context, sql_context=sql_context, hive_context=hive_context,
        config=config, date=processing_date
    )


def union_parts(info: ProcessingInfo) -> None:
    """Union flume parts into one file.

    :param info: info object.
    :return:
    """
    spark_context, config = info.spark_context, info.config

    spark_context.textFile(f"hdfs://{info.work_directory()}/{config['hadoop']['part_prefix']}*") \
        .coalesce(1) \
        .saveAsTextFile(
        f"hdfs://{info.work_directory()}/{config['hadoop']['union_directory']}"
    )


def is_union_success(info: ProcessingInfo) -> bool:
    """Return success union status.

    :param info: info object.
    :return:
    """
    spark_context, config = info.spark_context, info.config

    jvm = spark_context._jvm
    jsc = spark_context._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    return fs.exists(jvm.org.apache.hadoop.fs.Path(
        f"hdfs://{info.work_directory()}/{config['hadoop']['union_directory']}/_SUCCESS"
    ))


def get_row(row: pyspark.sql.Row) -> pyspark.sql.Row:
    """Mapping function for parse json.

    :param row: raw row.
    :return:
    """
    try:
        data = json.loads(row['value'])

        result = {'user_name': str(data['user_name'])}
        for key in ('anime_id', 'score', 'status'):
            result[key] = int(data[key])
    except:
        result = {key: None for key in ('user_name', 'anime_id', 'score', 'status')}

    result['_processing_date'] = PROCESSING_DATE
    return pyspark.sql.Row(**result)


def write_data_into_table(info: ProcessingInfo) -> None:
    """Write data into hive table.

    :param info: info object.
    :return:
    """
    spark_context, sql_context, hive_context, config = info.spark_context, info.sql_context, \
                                                       info.hive_context, info.config
    union_file = spark_context.textFile(
        f"hdfs://{info.work_directory()}/{config['hadoop']['union_directory']}/part-00000"
    )
    union_file_data_frame = union_file.toDF('string').rdd.map(get_row).toDF()

    union_file_data_frame.write\
        .format('hive').mode('append')\
        .insertInto(f'{config["hive"]["database"]}.{config["hive"]["table"]}')


if __name__ == '__main__':
    global PROCESSING_DATE

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout
    )
    LOGGER.setLevel(logging.INFO)

    info = build_precessing_info()
    assert info is not None

    PROCESSING_DATE = info.date

    # union_parts(info)

    if not is_union_success(info):
        LOGGER.error('Union parts operation is not success.')
    else:
        write_data_into_table(info)
