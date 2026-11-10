WITH CTE AS(
    SELECT * FROM {{ ref('stg_comments')}} 
)
SELECT 
    comment_id,
    post_id,
    email,
    LAG(comment_id) OVER(PARTITION BY post_id ORDER BY comment_id) AS previous_comment_id,
    (comment_id - LAG(comment_id) OVER(PARTITION BY post_id ORDER BY comment_id)) AS comment_id_gap
FROM CTE