WITH ranked_comments AS(
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY email ORDER BY comment_id DESC) AS rn,
    FROM {{ ref('stg_comments')}} 
)
SELECT comment_id, post_id, email, body
FROM ranked_comments
WHERE rn = 1
