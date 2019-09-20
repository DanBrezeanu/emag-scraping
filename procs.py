import sqlite3
from testing import WebScraper
from ok import DbManager
from utils import Logger, good_table_name
import multiprocessing
import time

class Worker:
    def __init__(self):
        self.db = DbManager.get_instance()
        self.scraper = WebScraper()
        self.cursor = self.db.get_cursor()

    def start_working(self, column, column_name, table):
        update_query = """UPDATE progress SET page = {}, products = {}, done = {} WHERE category = '{}'"""

        for pair in column:
            pair_total_products = self.scraper.get_total_products(pair[1])

            _query = """INSERT INTO progress VALUES ('{}', 0, 0, 0)""".format(pair[0])
            self.db.execute_query(_query, self.cursor, 'progress', 'initial', commit = True)

            n_prods = 0
            coroutine = self.scraper.all_prods_in_url(pair[1])

            while True:
                try:
                    page, prods = coroutine.send(None)
                except StopIteration:
                    break

                n_prods += len(prods)


                self.db.execute_query(update_query.format(page, n_prods, (0 if n_prods < pair_total_products else 1), pair[0]),
                                      self.cursor,
                                      'Updated progress',
                                      commit = True
                                      )


                for prod in prods:
                    query = """INSERT INTO {} VALUES ('{}','{}', {}, {}, '{}', '{}')""".format(
                        table,
                        prod.title.replace("'","''"),
                        prod.link,
                        (0 if prod.old_price is None else prod.old_price),
                        prod.new_price,
                        column_name,
                        pair[0]
                    )


                    self.db.execute_query(query, self.cursor)
                self.db.commit_queries(pair[1].split('/')[-2], page)


class MainProcess:
    def __init__(self, type):
        self.db = DbManager.get_instance()
        self.scraper = WebScraper()

        if type == 'fill_database':
            self.__init_fill_db()

    def __init_fill_db(self):
        depts = self.scraper.get_all_departments()

        for dept in depts:
            table_name = good_table_name(dept)
            self.db.create_table(table_name)

            workers = []
            for column_name in depts[dept]:
                worker = Worker()
                workers.append(worker)
                proc = multiprocessing.Process(target = worker.start_working,
                                               args = (depts[dept][column_name], column_name, table_name))

                proc.start()
                time.sleep(5)

if __name__ == '__main__':
    MainProcess('fill_database')
