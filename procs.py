import sqlite3
from testing import WebScraper
from ok import DbManager
from utils import Logger, good_table_name
import multiprocessing
import time

class Worker:
    def __init__(self):
        self.db = DbManager.getInstance()
        self.scraper = WebScraper()
        self.cursor = self.db.get_cursor()
        self.logger = Logger()


    def start_working(self, column, column_name, table):
        update_query = """UPDATE progress SET page = {}, products = {}, done = {} WHERE category = '{}'"""

        for pair in column:
            pair_total_products = self.scraper.get_total_products(pair[1])

            _query = """INSERT INTO progress VALUES ('{}', 0, 0, 0)""".format(pair[0])
            # self.db.execute_query(_query, self.cursor, 'progress', 'initial', commit = True)

            n_prods = 0
            coroutine = self.scraper.all_prods_in_url(pair[1])

            while True:
                try:
                    page, prods = coroutine.send(None)
                except StopIteration:
                    break

                n_prods += len(prods)

                #
                # self.db.execute_query(update_query.format(page, n_prods, (0 if n_prods < pair_total_products else 1), pair[0]),
                #                       self.cursor,
                #                       'Updated progress',
                #                       commit = True
                #                       )
                self.logger.committed_products(len(prods), pair[0])

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


                    self.db.execute_query(query, self.cursor, pair[1].split('/')[-2], page, commit = True)


class MainProcess:
    def __init__(self, type):
        self.db = DbManager.getInstance()
        self.scraper = WebScraper()
        self.logger = Logger()

        if type == 'fill_database':
            self.__init_fill_db()

    def __init_fill_db(self):
        depts = self.scraper.get_all_departments()
        procs = []

        jobs = [(depts[dept][column_name], column_name, good_table_name(dept)) for dept in depts for column_name in depts[dept]]

        workers, procs = [], []
        tables = []
        while len(jobs) > 0:
            job = jobs.pop()

            if job[2] not in tables:
                self.db.create_table(job[2])
                tables.append(job[2])

            worker = Worker()
            workers.append(worker)

            proc = multiprocessing.Process(target = worker.start_working, args = job)
            self.logger.starting_worker(*job[:0:-1])
            procs.append(proc)
            proc.start()

            time.sleep(5)

            for p in procs:
                p.join(timeout = 0)
                if not p.is_alive():
                    procs.remove(p)
                    break

if __name__ == '__main__':
    MainProcess('fill_database')
