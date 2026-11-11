with source as (
    select * from {{ source('public', 'raw_orders') }}
)
select
    order_id,
    customer_id,
    order_date::date as order_date,
    status,
    amount::numeric(10,2) as amount
from source