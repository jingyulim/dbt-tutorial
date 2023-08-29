-- with statement
with 

-- import CTEs
customers as (

  select * from {{ ref('stg_jaffle_shop__customers') }}

)

, orders as (

  select * from {{ ref('stg_jaffle_shop__orders') }}

)

, payments as (

  select * from {{ ref('stg_stripe__payments') }}

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
        orders.order_id
        , orders.customer_id
        , orders.order_placed_at
        , orders.order_status
        , failed_payments.total_amount_paid
        , failed_payments.payment_finalized_date
        , customers.customer_first_name
        , customers.customer_last_name

    from orders
    left join failed_payments on orders.order_id = failed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id
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