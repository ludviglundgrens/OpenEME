import sqlite3 as sql
import os

class engine_db():
    def __init__(self):
        if (os.path.exists('exhange_data.db')):
            os.remove('exhange_data.db')

        self.db = sql.connect('exhange_data.db')

    def create_tables(self):
        f = open('./sql/create_clob.sql')
        create_clob = f.read() #.replace('\n', '')
        self.db.execute(create_clob)

        f = open('./sql/create_tape.sql')
        create_tape = f.read() #.replace('\n', '')
        self.db.execute(create_tape)

    def fetch_data(self, string):
        cur = self.db.cursor()
        cur.execute(string)
        query_data = cur.fetchall()
        
        return(query_data)
    
    def insert_data(self, agent_id, symbol, buy_sell, quantity, price):
        cur = self.db.cursor()
        cur.execute("""INSERT INTO tape (agent_id, symbol, buy_sell, quantity, price) 
                       VALUES (?, ?, ?, ?, ?)""", (agent_id, symbol, buy_sell, quantity, price))
        self.db.commit()

    def test_data(self):
        cur = self.db.cursor()

        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (1, "BBB", "BUY", "10", "99");""")
        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (1,"BBB", "BUY", "10", "100");""")
        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (2,"BBB", "SELL", "10", "100");""")
        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (2,"BBB", "SELL", "15", "101");""")
        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (2,"BBB", "SELL", "10", "98");""")           
        cur.execute("""INSERT INTO clob (agent_id, symbol, buy_sell, quantity, price) 
                        VALUES (1, "BBB", "BUY", "20", "110");""")                  

        self.db.commit()

    def close(self):
        self.db.close()

if __name__ == '__main__':
    a = engine_db()
    a.create_tables()
    a.test_data()
    a.close()