-- with statement
with 

-- import CTEs
customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

)

, orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

)

, payments as (

  select * from {{ source('stripe', 'payments') }}

)

-- logical CTEs
, failed_payments as (
    select 
        orderid as order_id
        , max(created) as payment_finalized_date, sum(amount) / 100.0 as total_amount_paid        

    from payments
    where status <> 'fail'
    group by 1
)

, paid_orders as (
    select 
        orders.id as order_id
        , orders.user_id as customer_id
        , orders.order_date as order_placed_at
        , orders.status as order_status
        , failed_payments.total_amount_paid
        , failed_payments.payment_finalized_date
        , customers.first_name as customer_first_name
        , customers.last_name as customer_last_name

    from orders
    left join failed_payments on orders.id = failed_payments.order_id
    left join customers on orders.user_id = customers.id
)

-- final CTE
, final as (
    select
        paid_orders.*
        , row_number() over (order by paid_orders.order_id) as transaction_seq
        , row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq

        , case 
            when (
                -- customer_orders.first_order_date = paid_orders.order_placed_at 
                rank() over(partition by paid_orders.customer_id order by paid_orders.order_placed_at, paid_orders.order_id) = 1
            ) then 'new'
            else 'return' 
          end as nvsr
        
        , sum(total_amount_paid) over(partition by customer_id order by order_id rows between unbounded preceding and current row) as customer_lifetime_value

        -- first day of sale
        , first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id order by paid_orders.order_placed_at
          ) as fdos

    from paid_orders
    order by order_id
)

-- simple select statement
select * from final