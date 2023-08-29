-- with statement
with 

-- import CTEs
, customers as (

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
        ORDERID as order_id
        , max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid        

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

, order_clv as (
    select
        order_id
        , sum(total_amount_paid) over(partition by customer_id order by order_id rows between unbounded preceding and current row) as clv_bad
    
    from paid_orders
)

, customer_orders as (
    select 
        customers.id as customer_id
        , min(ORDER_DATE) as first_order_date
        , max(ORDER_DATE) as most_recent_order_date
        , count(ORDERS.ID) as number_of_orders

    from customers 
    left join orders on orders.user_id = customers.id 
    group by 1
)

-- final CTE
, final as (
    select
        paid_orders.*
        , row_number() over (order by paid_orders.order_id) as transaction_seq
        , row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq
        , case 
            when customer_orders.first_order_date = paid_orders.order_placed_at then 'new'
            else 'return' 
          end as nvsr
        , order_clv.clv_bad as customer_lifetime_value
        , customer_orders.first_order_date as fdos

    from paid_orders
    left join customer_orders using (customer_id)
    left outer join order_clv on order_clv.order_id = paid_orders.order_id

    order by order_id
)

-- simple select statement
select * from final