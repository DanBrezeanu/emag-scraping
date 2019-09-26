import psycopg2
from utils import Logger
import multiprocessing

class DbManager:
    __instance = None

    def __init__(self):
        if DbManager.__instance is None:
            self.lock = multiprocessing.Lock()
            self.conn = psycopg2.connect(host = '127.0.0.1', database = 'emag', user = 'danb', password = 'parola')
            # self.conn.autocommit = True

            self.create_table_prods_query = 'CREATE TABLE {} (link TEXT PRIMARY KEY NOT NULL, title TEXT, oldprice REAL, newprice REAL, column_name TEXT, category TEXT)'
            self.logger = Logger()
            DbManager.__instance = self
            self.cursor = self.conn.cursor()
        else:
            raise RuntimeError('DbManager constructor called multiple times')

    @staticmethod
    def get_instance():
        if DbManager.__instance is None:
            DbManager()

        return DbManager.__instance

    def create_table(self, table_name):
        self.lock.acquire()
        # try:
        self.cursor.execute(self.create_table_prods_query.format(table_name))

        # except Exception as e:
        #     print('CANT CREATE TABLE {}'.format(table_name))
        #
        #     try:
        #         self.cursor.close()
        #         self.cursor = self.conn.cursor()
        #     except:
        #         self.conn.close()
        #         self.conn = psycopg2.connect(host = '127.0.0.1', database = 'emag', user = 'danb', password = 'parola')
        #
        #     self.cursor = self.conn.cursor()

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
