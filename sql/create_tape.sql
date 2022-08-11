CREATE TABLE tape (
    id integer PRIMARY KEY AUTOINCREMENT,
    agent_id integer,
    time timestamp default (strftime('%s', 'now')), 
    symbol text not null, 
    buy_sell text not null, 
    quantity real not null, 
    price real not null
);