import setup
import time

class engine():
    def __init__(self, heartbeat_time) :
        self.heartbeat_time = heartbeat_time
        self.last_runtime = 0

        # Setup database with database class from setup file
        self.db = setup.engine_db()
        self.db.create_tables()
        self.db.test_data()

    def matching_process(self):
        pass

    def process_handler(self):
        current_time = time.time()
        if current_time - self.last_runtime > self.heartbeat_time:
            res = self.db.query("select * from clob")
            print(res)

if __name__ == '__main__':
    a = engine(0.5)
    time.sleep(1)
    a.process_handler()
