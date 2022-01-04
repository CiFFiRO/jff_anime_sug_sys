from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StringType, IntegerType,
    StructField
)
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, last, sum, log
import argparse


def build_ratings(spark_session: SparkSession, file_path: str) -> DataFrame:
    """Convert csv scores data to dataframe.

    :param spark_session: SparkSession object.
    :param file_path: path to raw csv data.
    :return:
    """
    schema = StructType([
        StructField(name, type_, True)
        for name, type_ in (
            ('username', StringType()), ('anime_id', IntegerType()),
            ('my_watched_episodes', IntegerType()), ('my_start_date', StringType()),
            ('my_finish_date', StringType()), ('my_score', IntegerType()),
            ('my_status', IntegerType()), ('my_rewatching', IntegerType()),
            ('my_rewatching_ep', IntegerType()), ('my_last_updated', IntegerType()),
            ('my_tags', StringType())
        )
    ])

    rating_df = spark_session.read.csv(file_path, header=True, schema=schema)
    result = rating_df.select('username', 'anime_id', 'my_score', 'my_status') \
        .where(
            (col('username').isNotNull()) & (col('anime_id').isNotNull()) &
            (col('my_score').isNotNull()) & (col('my_status').isNotNull())
        ) \
        .where(col('my_status') == 2) \
        .select('username', 'anime_id', 'my_score') \
        .groupBy('username', 'anime_id') \
        .agg(last('my_score').alias('score')) \
        .select('username', 'anime_id', 'score')

    return result


def build_my_list(spark_session: SparkSession, file_path: str) -> DataFrame:
    """Convert my csv scores data to dataframe.

    :param spark_session: SparkSession object.
    :param file_path: path to raw csv data.
    :return:
    """
    schema = StructType([
        StructField(name, type_, True)
        for name, type_ in (('anime_id', IntegerType()), ('my_score', IntegerType()))
    ])

    rating_df = spark_session.read.csv(file_path, header=True, schema=schema)
    result = rating_df.select('anime_id', col('my_score').alias('score')) \
        .where(
            (col('anime_id').isNotNull()) & (col('score').isNotNull())
        ) \
        .select('anime_id', 'score') \
        .groupBy('anime_id') \
        .agg(last('score').alias('score')) \
        .select('anime_id', 'score')

    return result


def collaborative_filtering(ratings: DataFrame, my_list: DataFrame, number_nearest: int = 10000) -> DataFrame:
    """Return user-based anime suggestions.

    :param ratings: other users scores dataframe.
    :param my_list: my scores dataframe.
    :param number_nearest: number nearest users used.
    :return:
    """
    top_cos_distance = ratings.join(my_list, on='anime_id') \
        .select(
            ratings.username.alias('user_name'), ratings.score.alias('ratings_score'),
            my_list.score.alias('my_list_score'), ratings.anime_id.alias('anime_id')
        ) \
        .groupBy('user_name') \
        .agg((log(sum(col('ratings_score') * col('my_list_score'))) -
              0.5 * log(sum(col('ratings_score') * col('ratings_score')) +
                        sum(col('my_list_score') * col('my_list_score')))).alias('distance')) \
        .select('user_name', 'distance') \
        .orderBy(col('distance').desc()) \
        .limit(number_nearest)

    weight = top_cos_distance.join(ratings, top_cos_distance.user_name == ratings.username) \
        .select(
            ratings.username.alias('user_name'),
            ratings.anime_id.alias('anime_id'),
            (ratings.score * top_cos_distance.distance).alias('w')
        )

    sum_distance = weight.agg(sum(col('w')).alias('sum_w')) \
        .select('sum_w')

    return weight.join(my_list, on='anime_id', how='left') \
        .select(
            weight.anime_id.alias('anime_id'), my_list.score.alias('my_score'),
            weight.w.alias('w')
        ) \
        .where(col('my_score').isNull()) \
        .groupBy('anime_id') \
        .agg((sum(col('w')) / sum_distance.first()['sum_w']).alias('rating')) \
        .select('anime_id', 'rating') \
        .orderBy(col('rating').desc()) \
        .limit(15)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores-path", help="Path to scores data csv.")
    parser.add_argument("--my-list-path", help="Path to my list data csv.")
    args = parser.parse_args()

    if args.scores_path is not None and args.my_list_path is not None:
        scores_path = args.scores_path
        my_list_path = args.my_list_path
    else:
        raise Exception('Bad arguments.')

    spark_session = SparkSession.builder \
        .master('yarn') \
        .appName('pythonSpark') \
        .getOrCreate()

    ratings = build_ratings(spark_session, scores_path)
    my_list = build_my_list(spark_session, my_list_path)

    suggestions = collaborative_filtering(ratings, my_list)
    suggestions.show(n=25, truncate=False)


if __name__ == '__main__':
    main()
