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
-- final CTE
-- simple select statement

paid_orders as (
    select 
        Orders.ID as order_id
        , Orders.USER_ID as customer_id
        , Orders.ORDER_DATE as order_placed_at
        , Orders.STATUS as order_status
        , p.total_amount_paid
        , p.payment_finalized_date
        , C.FIRST_NAME as customer_first_name
        , C.LAST_NAME as customer_last_name

    from orders
    left join (
        select 
            ORDERID as order_id
            , max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid        

        from payments
        where status <> 'fail'
        group by 1
    ) p on orders.ID = p.order_id
    left join customers C on orders.USER_ID = C.ID 
),

customer_orders as (
    select 
        C.ID as customer_id
        , min(ORDER_DATE) as first_order_date
        , max(ORDER_DATE) as most_recent_order_date
        , count(ORDERS.ID) as number_of_orders

    from customers C 
    left join orders on orders.USER_ID = C.ID 
    group by 1
)

select
    p.*
    , row_number() over (order by p.order_id) as transaction_seq
    , ROW_NUMBER() over (partition by customer_id order by p.order_id) as customer_sales_seq
    , case 
        when c.first_order_date = p.order_placed_at then 'new'
        else 'return' 
      end as nvsr
    , x.clv_bad as customer_lifetime_value
    , c.first_order_date as fdos

from paid_orders p
left join customer_orders as c using (customer_id)
left outer join (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    
    from paid_orders p
    left join paid_orders t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id

order by order_id