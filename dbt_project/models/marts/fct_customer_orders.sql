with orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'completed'
),
customers as (
    select * from {{ source('public', 'raw_customers') }}
)
select
    c.customer_id,
    c.name,
    c.country,
    count(o.order_id) as total_orders,
    coalesce(sum(o.amount), 0) as total_spent
from customers c
left join orders o on c.customer_id = o.customer_id
group by c.customer_id, c.name, c.country