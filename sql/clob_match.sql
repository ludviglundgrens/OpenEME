with buy_orders as (
SELECT 
    id,
    agent_id,
    time,
    symbol,
    buy_sell,
    quantity,
    price,
    Sum(quantity) Over ( Order by price desc) As cumq
FROM clob
where buy_sell = 'BUY'
),
sell_orders as (
SELECT 
    id,
    agent_id,
    time,
    symbol,
    buy_sell,
    quantity,
    price,
    Sum(quantity) Over ( Order by price) As cumq
FROM clob
where buy_sell = 'SELL'
),
buy_orders_match as (
select *
from buy_orders 
where 
    price >= (select min(price) from sell_orders)
),
sell_orders_match as (
select * 
from sell_orders 
where  
    price <= (select max(price) from buy_orders) 
)
select * 
from buy_orders_match
where (cumq - quantity) < (select sum(quantity) from sell_orders_match)
union all
select * from sell_orders_match
where (cumq - quantity) < (select sum(quantity) from buy_orders_match)