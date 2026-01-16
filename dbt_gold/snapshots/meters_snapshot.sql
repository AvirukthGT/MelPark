{% snapshot meters_snapshot %}

{{
    config(
      target_database='databricks',
      target_schema='gold',
      unique_key='meter_id',
      strategy='check',
      check_cols=['has_credit_card', 'has_tap_and_go', 'asset_id'],
    )
}}

select * from {{ source('melpark', 'parking_meters') }}

{% endsnapshot %}