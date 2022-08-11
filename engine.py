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
        clob_match = self.db.fetch_data(clob_match_sql)
        print(clob_match)
        clob_match = pd.DataFrame(clob_match, columns = list(['id', 'agent_id','time', 'symbol', 'buy_sell', 'quantity', 'price', 'cumq']))
        
        buy_orders = clob_match.query('buy_sell == "BUY"').sort_values('price', ascending=False)
        sell_orders = clob_match.query('buy_sell == "SELL"').sort_values('price', ascending=True)
        
        buy_orders = buy_orders.assign(filled = 0).reset_index(drop = True)
        sell_orders = sell_orders.assign(filled = 0).reset_index(drop = True)
        while(len(buy_orders.index) > 0 and len(buy_orders.index > 0)):
            
            #print(buy_orders)
            #print(sell_orders)
            
            if (buy_orders["quantity"][0]-buy_orders["filled"][0]) == (sell_orders["quantity"][0]-sell_orders["filled"][0]):
                buy_orders.at[0, 'filled'] = buy_orders.at[0, 'quantity']
                sell_orders.at[0, 'filled'] = sell_orders.at[0, 'quantity']

                print("order", buy_orders.at[0, 'id'], "was filled")
                print("order", sell_orders.at[0, 'id'], "was filled")

                # Price in transaction, time priority
                if buy_orders.at[0, 'time'] < sell_orders.at[0, 'time']:
                    price = buy_orders.at[0, 'price']
                else:
                    price = sell_orders.at[0, 'price']
                
                # Insert data into tape
                self.db.insert_data(agent_id = int(buy_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "BUY", quantity = buy_orders.at[0, 'price'], price = price)
                self.db.insert_data(agent_id = int(sell_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "SELL", quantity = sell_orders.at[0, 'price'], price = price)

                if len(buy_orders.index) > 1:
                    buy_orders = buy_orders.iloc[1:, :].reset_index(drop = True)
                else:
                    break
                if len(sell_orders.index) > 1:
                    sell_orders = sell_orders.iloc[1:, :].reset_index(drop = True)
                else: 
                    break
                
            elif (buy_orders["quantity"][0]-buy_orders["filled"][0]) < (sell_orders["quantity"][0]-sell_orders["filled"][0]):
                buy_orders.at[0, 'filled'] = buy_orders.at[0, 'quantity']
                sell_orders.at[0, 'filled'] = buy_orders.at[0, 'quantity']

                print("order", buy_orders.at[0, 'id'], "was filled")

                # Get price in transaction, time priority
                if buy_orders.at[0, 'time'] < sell_orders.at[0, 'time']:
                    price = buy_orders.at[0, 'price']
                else:
                    price = sell_orders.at[0, 'price']

                # Insert data into tape
                self.db.insert_data(agent_id = int(buy_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "BUY", quantity = buy_orders.at[0, 'price'], price = price)
                self.db.insert_data(agent_id = int(sell_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "SELL", quantity = buy_orders.at[0, 'price'], price = price)

                if len(buy_orders.index) > 1:
                    buy_orders = buy_orders.iloc[1:, :].reset_index(drop = True)
                else:
                    break

            elif (buy_orders["quantity"][0]-buy_orders["filled"][0]) > (sell_orders["quantity"][0]-sell_orders["filled"][0]):
                sell_orders.at[0, 'filled'] = sell_orders.at[0, 'quantity']
                buy_orders.at[0, 'filled'] = sell_orders.at[0, 'quantity']

                print("order", sell_orders.at[0, 'id'], "was filled")

                # Price in transaction, time priority
                if buy_orders.at[0, 'time'] < sell_orders.at[0, 'time']:
                    price = buy_orders.at[0, 'price']
                else:
                    price = sell_orders.at[0, 'price']

                # Insert data into tape
                self.db.insert_data(agent_id = int(buy_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "BUY", quantity = sell_orders.at[0, 'price'], price = price)
                self.db.insert_data(agent_id = int(sell_orders.at[0, 'agent_id']), symbol = "AAA", buy_sell = "SELL", quantity = sell_orders.at[0, 'price'], price = price)
                
                if len(sell_orders.index) > 1:
                    sell_orders = sell_orders.iloc[1:, :].reset_index(drop = True)
                else:
                    break

if __name__ == '__main__':
    a = engine(0.5)
    time.sleep(1)
    a.process_handler()
