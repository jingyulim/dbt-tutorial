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
        order_id
        , max(payment_created_at) as payment_finalized_date
        , sum(payment_amount) as total_amount_paid        

    from payments
    where payment_status <> 'fail'
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
        *
        , row_number() over (order by order_id) as transaction_seq
        , row_number() over (partition by customer_id order by order_id) as customer_sales_seq

        , case 
            when (
                rank() over(partition by customer_id order by order_placed_at, order_id) = 1
            ) then 'new'
            else 'return' 
          end as nvsr
        
        , sum(total_amount_paid) over(partition by customer_id order by order_id rows between unbounded preceding and current row) as customer_lifetime_value

        -- first day of sale
        , first_value(order_placed_at) over (
            partition by customer_id order by order_placed_at
          ) as fdos

    from paid_orders
    order by order_id
)

-- simple select statement
select * from final