WITH CTE AS(
    SELECT * FROM {{ ref('stg_comments')}} 
)
SELECT 
comment_id,
post_id,
email,
ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY comment_id) AS comment_number_per_post,
COUNT(comment_id) OVER(PARTITION BY post_id) AS total_comments_on_post
FROM CTE