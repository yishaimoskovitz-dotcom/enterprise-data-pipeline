with source_data as (
    select * from {{ source('raw_data', 'raw_comments') }}
)

select
    cast(id as int) as comment_id,
    cast(postId as int) as post_id,
    trim(name) as name,
    lower(trim(email)) as email,
    {{ clean_string('body') }} as body
from source_data