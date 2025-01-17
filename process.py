from webscraper import WebScraper
from database import DbManager, Query
from settings import Settings
from utils import Logger, good_table_name

if __name__ == '__main__':
    from scheduler import Scheduler
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
            if pair[0] in Settings.BANNED_PAIRS:
                continue

            done, pages, products = self.db.get_progress_for_pair(pair[0])
            if done:
                continue

            pair_total_products = self.scraper.get_total_products(pair[1])

            n_prods = 0
            coroutine = self.scraper.all_prods_in_url(pair[1], pages, products)
            self.db.execute_query(Query.init_progress_category.format(pair[0]))

            while True:
                try:
                    page, prods = coroutine.send(None)
                except StopIteration:
                    break

                n_prods += len(prods)


                for prod in prods:
                    self.db.execute_query(Query.insert_products.format(
                        self.tile_name,
                        prod.link,
                        prod.title.replace("'","''"),
                        (0 if prod.old_price is None else prod.old_price),
                        prod.new_price,
                        self.column_name,
                        pair[0]
                    ))


                self.logger.committed_products(len(prods), pair[0], page)

                self.db.execute_query(Query.update_progress_table.format(page, n_prods, pair[0]))
            self.db.execute_query(Query.update_progress_done_column.format(pair[0]))

class MainProcess:
    def __init__(self, type):
        self.scraper = WebScraper()
        self.logger = Logger()
        self.db = DbManager.get_instance()

        if type == 'fill_database':
            self.__init_fill_db()

    def __init_fill_db(self):
        Scheduler()

if __name__ == '__main__':
    MainProcess('fill_database')
