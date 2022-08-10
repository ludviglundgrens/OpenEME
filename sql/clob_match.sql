with buy_orders as (
SELECT *
FROM clob
where buy_sell = 'BUY'
),
sell_orders as (
SELECT *
FROM clob
where buy_sell = 'SELL'
)
select * 
from buy_orders 
where price >= (select min(price) from sell_orders)
union all
select * 
from sell_orders 
where price <= (select max(price) from buy_orders)
