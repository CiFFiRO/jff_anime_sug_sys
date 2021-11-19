with my_list as (select * from anime.my_completed_list),
    cos_dist as (
        select
            completed_list.user_name as user_name,
            coalesce(
                ln(sum(completed_list.score * my_list.score)) -
                0.5 * (ln(sum(completed_list.score * completed_list.score)) + ln(sum(my_list.score * my_list.score))),
                0.0
            ) as dist
        from anime.completed_list as completed_list left join my_list on
            completed_list.anime_id = my_list.anime_id
        where my_list.anime_id is not null
        group by completed_list.user_name
    ),
    top_cos_dist as (
        select * from cos_dist order by dist desc limit 10000
    ),
    target_table as (
        select
            completed_list.user_name as user_name,
            completed_list.anime_id as anime_id,
            completed_list.score * top_cos_dist.dist as w
        from anime.completed_list as completed_list inner join top_cos_dist on
            completed_list.user_name = top_cos_dist.user_name
    ),
    target_items as (
        select
            target_table.anime_id as anime_id,
            sum(w) / (select sum(dist) from top_cos_dist) as rate
        from target_table left join my_list on
            target_table.anime_id = my_list.anime_id
        where my_list.anime_id is null
        group by
            target_table.anime_id
    )

insert overwrite table anime.suggestion
    select * from target_items order by rate desc
;


