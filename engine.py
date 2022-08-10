import setup
import time
import pandas as pd

class engine():
    def __init__(self, heartbeat_time) :
        self.heartbeat_time = heartbeat_time
        self.last_runtime = 0

        # Setup database with database class from setup file
        self.db = setup.engine_db()
        self.db.create_tables()
        self.db.test_data()

    def process_handler(self):
        current_time = time.time()
        if current_time - self.last_runtime > self.heartbeat_time:
            self.matching_process()

    def matching_process(self):
        f = open('./sql/clob_match.sql')
        clob_match_sql = f.read()
        clob_match = self.db.query(clob_match_sql)
        
        clob_match = pd.DataFrame(clob_match, columns = list(['id', 'time', 'symbol', 'buy_sell', 'quantity', 'price']))
        
        buy_orders = clob_match.query('buy_sell == BUY').sort_values('price', ascending=False)
        sell_orders = clob_match.query('buy_sell == SELL').sort_values('price', ascending=True)
        
        buy_orders.assign(filled = 0)
        sell_orders.assign(filled = 0)


if __name__ == '__main__':
    a = engine(0.5)
    time.sleep(1)
    a.process_handler()
