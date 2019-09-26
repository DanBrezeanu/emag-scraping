
from testing import WebScraper
from ok import DbManager
from utils import Logger, good_table_name
import multiprocessing
import time

class Worker:
    def __init__(self, tile_name, column_name, pairs):
        self.db = DbManager.get_instance()
        self.scraper = WebScraper()
        self.cursor = None
        self.logger = Logger()
        self.column_name = column_name
        self.tile_name = tile_name
        self.pairs = pairs

    def start_working(self):
        for pair in self.pairs:
            pair_total_products = self.scraper.get_total_products(pair[1])

            n_prods = 0
            coroutine = self.scraper.all_prods_in_url(pair[1])

            while True:
                try:
                    page, prods = coroutine.send(None)
                except StopIteration:
                    break

                n_prods += len(prods)

                self.logger.committed_products(len(prods), pair[0])

                for prod in prods:
                    query = """INSERT INTO {} VALUES ('{}','{}', {}, {}, '{}', '{}')""".format(
                        self.tile_name,
                        prod.link,
                        prod.title.replace("'","''"),
                        (0 if prod.old_price is None else prod.old_price),
                        prod.new_price,
                        self.column_name,
                        pair[0]
                    )
                    self.db.execute_query(query)

                # self.db.commit_queries()

class MainProcess:
    def __init__(self, type):
        self.scraper = WebScraper()
        self.logger = Logger()
        self.db = DbManager.get_instance()

        if type == 'fill_database':
            self.__init_fill_db()

    def __init_fill_db(self):
        depts = self.scraper.get_all_departments()


        #         # [pair]                     col name           tile name
        # jobs = [(depts[dept][column_name], column_name, good_table_name(dept)) for dept in depts for column_name in depts[dept]]

        workers = []

        for tile in depts:
            self.db.create_table(good_table_name(tile))
            for column in depts[tile]:
                worker = Worker(good_table_name(tile), column, depts[tile][column])

                proc = multiprocessing.Process(target = worker.start_working)
                # self.logger.starting_worker(*job[:0:-1])

                workers.append((worker, proc))
                proc.start()
                time.sleep(5)

                while len(workers) >= 4:
                    for w, p in workers:
                        p.join(timeout = 0)
                        if not p.is_alive():
                            workers.remove((w, p))
                            break


if __name__ == '__main__':
    MainProcess('fill_database')
