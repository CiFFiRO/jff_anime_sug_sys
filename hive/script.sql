create database if not exists `anime`;

create table if not exists `anime`.`user_score` (
    `user_name` string,
    `anime_id` bigint,
    `score` tinyint,
    `status` tinyint
)
partitioned by (`_processing_date` string)
;

