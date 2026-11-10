with comments as (
    select * from {{ ref('stg_comments') }}
)

select
    email,
    count(comment_id) as total_comments
from comments
group by email