import psycopg2
from utils import Logger
from settings import Settings
import multiprocessing

class Query:
    create_table_prods = 'CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, link TEXT NOT NULL, title TEXT, oldprice REAL, newprice REAL, column_name TEXT, category TEXT);'
    create_progress_table = 'CREATE TABLE IF NOT EXISTS progress (category TEXT PRIMARY KEY NOT NULL, pages INTEGER, products INTEGER, locked INTEGER, done INTEGER);'
    update_progress_table = "UPDATE progress SET pages = {}, products = {} WHERE category = '{}';"
    update_progress_done_column = "UPDATE progress SET done = 1 WHERE category = '{}';"
    # update_progress_done_column = "UPDATE progress SET locked = {} WHERE category = '{}';"
    init_progress_category = "INSERT INTO progress VALUES ('{}', 0, 0, 0, 0)"
    get_all_progress = "SELECT * FROM progress;"
    insert_products = "INSERT INTO {} (link, title, oldprice, newprice, column_name, category) VALUES ('{}','{}', {}, {}, '{}', '{}')"

class DbManager:
    __instance = None

    def __init__(self):
        if DbManager.__instance is None:
            self.lock = multiprocessing.Lock()
            self.conn = psycopg2.connect(host = Settings.DATABASE_HOSTNAME, database = Settings.DATABASE_NAME, user = 'danb', password = 'parola')
            self.logger = Logger()
            DbManager.__instance = self

            self.cursor = self.conn.cursor()
            self.execute_query(Query.create_progress_table)
            self.execute_query(Query.get_all_progress)
            self.all_progress = self.cursor.fetchall()
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

    def get_progress_for_pair(self, pair_name):
        if not any(pair_name in pair_progress for pair_progress in self.all_progress):
            return False, 0, 0
        else:
            for pair_progress in self.all_progress:
                if pair_progress[0] == pair_name:
                    return pair_progress[-1], pair_progress[1], pair_progress[2]

    def check_progress(self):
        import datetime
        self.cursor.execute('SELECT * FROM progress;')

        timenow = ''.join(str(datetime.datetime.now()).split('.')[0])
        print('-' * 50)
        print(timenow)
        print('-' * 50)

        try:
            results = self.cursor.fetchall()
            for result in results:
                print('{:>30}  {:>5}  {:>6} {}'.format(result[0], result[1], result[2], 'DONE' if result[3] == 1 else 'IN PROGRESS'))
        except:
            print()
        print('-' * 50)

        try:
            self.cursor.execute('SELECT SUM(products) FROM progress;')
            results = self.cursor.fetchall()[0]
            print('{:>30}  {:>5}  {:>6}'.format('Total', ' ', results[0]))
        except:
            print('{:>30}  {:>5}  {:>6}'.format('Total', '', 0))

        print('-' * 50)
