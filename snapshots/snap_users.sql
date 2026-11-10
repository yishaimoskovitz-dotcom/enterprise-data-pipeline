{% snapshot snap_users %}

{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='check',
      check_cols=['email', 'name'],
    )
}}

select * from {{ source('raw_data', 'raw_users') }}

{% endsnapshot %}