with batch_completed_list as (
	select distinct
	    user_name,
	    anime_id,
	    last_value(score) over (PARTITION BY user_name, anime_id) as score
	from ${batch_table}
	where status = 2
)

insert overwrite table anime.completed_list
    select
        completed_list.user_name as user_name,
        completed_list.anime_id as anime_id,
        coalesce(batch_completed_list.score, completed_list.score) as score
    from anime.completed_list as completed_list left join batch_completed_list on
        completed_list.user_name = batch_completed_list.user_name
        and completed_list.anime_id = batch_completed_list.anime_id;



