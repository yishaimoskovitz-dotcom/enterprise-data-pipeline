WITH CTE AS( 
SELECT * FROM {{ source('raw_data', 'raw_api_posts') }}
)
SELECT 
    CAST(id AS INT) AS api_post_id,
    CAST(userId AS INT) AS user_id,
    {{ clean_string('title') }} AS title,
    {{ clean_string('body') }} AS body
FROM CTE
