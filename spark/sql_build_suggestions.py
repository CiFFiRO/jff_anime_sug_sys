from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum
import argparse


def build_ratings(spark_session: SparkSession, file_path: str) -> DataFrame:
    """Convert csv scores data to dataframe.

    :param spark_session: SparkSession object.
    :param file_path: path to raw csv data.
    :return:
    """
    schema = "`username` string, `anime_id` int, `my_watched_episodes` int, `my_start_date` string, " \
        "`my_finish_date` string, `my_score` int, `my_status` int, `my_rewatching` int, `my_rewatching_ep` int, " \
        "`my_last_updated` int, `my_tags` string"
    tmp_table_name = 'anime_scores'

    rating_df = spark_session.read.format('csv') \
        .option("header", "true") \
        .option("customSchema", schema) \
        .load(file_path)
    rating_df.createOrReplaceTempView(tmp_table_name)

    return spark_session.sql(f"""
        select `username`, `anime_id`, last(`my_score`) as `score` 
        from `{tmp_table_name}`
        where `username` is not null 
            and `anime_id` is not null
            and `my_score` is not null
            and `my_status` is not null
            and if(`my_status` is not null, `my_status` = 2, false)
        group by `username`, `anime_id`
    """)


def build_my_list(spark_session: SparkSession, file_path: str) -> DataFrame:
    """Convert my csv scores data to dataframe.

    :param spark_session: SparkSession object.
    :param file_path: path to raw csv data.
    :return:
    """
    schema = "`anime_id` int, `my_score` int"
    tmp_table_name = 'my_anime_scores'

    rating_df = spark_session.read.format('csv') \
        .option("header", "true") \
        .option("customSchema", schema) \
        .load(file_path)
    rating_df.createOrReplaceTempView(tmp_table_name)

    return spark_session.sql(f"""
        select `anime_id`, last(`my_score`) as `score` 
        from `{tmp_table_name}`
        where `anime_id` is not null
            and `my_score` is not null
        group by `anime_id`
    """)


def collaborative_filtering(
        spark_session: SparkSession, ratings: DataFrame,
        my_list: DataFrame, number_nearest: int = 10000
) -> DataFrame:
    """Return user-based anime suggestions.

    :param spark_session: SparkSession object.
    :param ratings: other users scores dataframe.
    :param my_list: my scores dataframe.
    :param number_nearest: number nearest users used.
    :return:
    """
    tmp_ratings_table_name = 'anime_ratings'
    tmp_my_list_table_name = 'my_anime_list'
    tmp_distance_table_name = 'top_cos_distance'
    tmp_weight_table_name = 'weight'

    ratings.createOrReplaceTempView(tmp_ratings_table_name)
    my_list.createOrReplaceTempView(tmp_my_list_table_name)

    top_cos_distance = spark_session.sql(f"""
        select
            cos_distance.`username` as `user_name`,
            cos_distance.`distance` as `distance` 
        from (
            select
                rating.`username` as `username`,
                log(sum(rating.`score` * my_list.`score`)) -
                  0.5 * log(sum(rating.`score` * rating.`score`) + sum(my_list.`score` * my_list.`score`)) as `distance` 
            from `{tmp_ratings_table_name}` as rating join `{tmp_my_list_table_name}` as my_list using(`anime_id`)
            group by rating.`username`
        ) as cos_distance
        order by cos_distance.`distance` desc 
        limit {number_nearest}
    """)
    top_cos_distance.createOrReplaceTempView(tmp_distance_table_name)

    weight = spark_session.sql(f"""
        select 
            rating.username as `user_name`,
            rating.anime_id as `anime_id`,
            rating.score * top_cos_distance.distance as `w`
        from {tmp_distance_table_name} as top_cos_distance join {tmp_ratings_table_name} as rating on 
            top_cos_distance.`user_name` == rating.`username`
    """)
    weight.createOrReplaceTempView(tmp_weight_table_name)

    sum_distance = weight.agg(sum(col('w')).alias('sum_w')) \
        .select('sum_w')

    return spark_session.sql(f"""
        select 
            rate.`anime_id` as `anime_id`, 
            rate.`rating` as `rating`
        from (
            select 
                weight.`anime_id` as `anime_id`,
                sum(weight.`w`) / {sum_distance.first()['sum_w']} as `rating`
            from {tmp_weight_table_name} as weight left join {tmp_my_list_table_name} as my_list using(`anime_id`)
            where my_list.`score` is null 
            group by weight.`anime_id`
        ) as rate
        order by rate.`rating` desc
    """)


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

    suggestions = collaborative_filtering(spark_session, ratings, my_list)
    suggestions.show(n=25, truncate=False)


if __name__ == '__main__':
    main()
