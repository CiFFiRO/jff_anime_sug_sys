create table if not exists anime.completed_list (
    `user_name` string,
    `anime_id` bigint,
    `score` bigint
);

create table if not exists anime.suggestion (
    `anime_id` bigint,
    `rate` double
);
