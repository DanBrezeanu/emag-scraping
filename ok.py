import sqlite3
from utils import Logger
import multiprocessing


class DbManager:
    __instance = None

    def __init__(self):
        if DbManager.__instance is None:
            DbManager.__instance = self
            self.conn = sqlite3.connect('db.sqlite3')
            self.create_table_prods_query = 'CREATE TABLE {} (link text, title text, oldprice real, newprice real, column_name text, category text)'
            self.logger = Logger()
            self.lock = multiprocessing.Lock()

            self.__init_progress()
        else:
            raise Exception('Multiple DbManger instances created')

    @staticmethod
    def getInstance():
        if DbManager.__instance is None:
            DbManager()

        return DbManager.__instance

    def __init_progress(self):
        try:
            self.conn.cursor().execute('CREATE TABLE progress (category text, page integer, products integer, done integer)')
        except sqlite3.OperationalError as e:
            self.logger.table_already_exists('progress', extra = 'Loading progress')
            #TODO:

    def commit_queries(self, *args):
        while True:
            try:
                self.lock.acquire()
                self.conn.commit()
                self.lock.release()
                break
            except sqlite3.OperationalError as e:
                self.lock.release()
                self.logger.failed_to_commit(*args)

    def create_table(self, table_name):
        try:
            self.conn.cursor().execute(self.create_table_prods_query.format(table_name))
            self.commit_queries('create_table', table_name)
        except sqlite3.OperationalError as e:
            self.logger.table_already_exists(table_name)

    def execute_query(self, query, cursor, *args, commit = False):
        try:
            cursor.execute(query)
        except sqlite3.OperationalError as e:
            self.logger.failed_to_execute(e)
            return

        if commit:
            self.commit_queries(*args)

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


    def get_cursor(self):
        return self.conn.cursor()
