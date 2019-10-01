import psycopg2
from utils import Logger
import multiprocessing

class Query:
    create_table_prods = 'CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, link TEXT NOT NULL, title TEXT, oldprice REAL, newprice REAL, column_name TEXT, category TEXT);'
    create_progress_table = 'CREATE TABLE IF NOT EXISTS progress (category TEXT PRIMARY KEY NOT NULL, pages INTEGER, products INTEGER, done INTEGER);'
    update_progress_table = "UPDATE progress SET pages = {}, products = {}, done = {} WHERE category = '{}';"
    init_progress_category = "INSERT INTO progress VALUES ('{}', 0, 0, 0)"

class DbManager:
    __instance = None

    def __init__(self):
        if DbManager.__instance is None:
            self.lock = multiprocessing.Lock()
            self.conn = psycopg2.connect(host = '127.0.0.1', database = 'emag', user = 'danb', password = 'parola')
            self.logger = Logger()
            DbManager.__instance = self

            self.cursor = self.conn.cursor()
            self.execute_query(Query.create_progress_table)
        else:
            raise RuntimeError('DbManager constructor called multiple times')

    @staticmethod
    def get_instance():
        if DbManager.__instance is None:
            DbManager()

        return DbManager.__instance

    def create_table(self, table_name):
        self.lock.acquire()
        self.cursor.execute(Query.create_table_prods.format(table_name))

        self.conn.commit()
        self.lock.release()

    def execute_query(self, query, *args, commit = False):
        self.lock.acquire()

        try:
            self.cursor.execute(query)
        except Exception as e:
            self.logger.failed_to_execute(e, query = query)
            try:
                self.cursor.close()
                self.cursor = self.conn.cursor()
            except:
                self.conn.close()
                self.conn = psycopg2.connect(host = '127.0.0.1', database = 'emag', user = 'danb', password = 'parola')
            self.cursor = self.conn.cursor()

        self.conn.commit()
        self.lock.release()

    def check_progress(self):
        import datetime
        c = self.conn.cursor()

        timenow = ''.join(str(datetime.datetime.now()).split('.')[0])
        print('-' * 50)
        print(timenow)
        print('-' * 50)

        try:
            results = c.execute('SELECT * FROM progress').fetchall()
            for result in results:
                print('{:>30}  {:>5}  {:>6} {}'.format(result[0], result[1], result[2], 'DONE' if result[3] == 1 else 'IN PROGRESS'))
        except:
            print()
        print('-' * 50)

        try:
            results = c.execute('SELECT SUM(products) FROM progress').fetchall()[0]
            print('{:>30}  {:>5}  {:>6}'.format('Total', ' ', results[0]))
        except:
            print('{:>30}  {:>5}  {:>6}'.format('Total', '', 0))

        print('-' * 50)
    #
    # def get_cursor(self):
    #     return self.conn.cursor()
