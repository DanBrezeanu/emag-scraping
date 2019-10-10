from settings import Settings
from webscraper import WebScraper
import multiprocessing
from database import DbManager
import time
import datetime
import subprocess
from process import Worker
from utils import good_table_name, Logger

class Scheduler:
    def __init__(self):
        available_hosts = ['rpi1', 'rpi2']

        self.logger = Logger()
        self.scraper = WebScraper()
        self.db = DbManager.get_instance()
        depts = self.scraper.get_all_departments()
        workers = []


        for tile in depts:
            if good_table_name(tile) in Settings.BANNED_TILES:
                continue
            self.db.create_table(good_table_name(tile))
            for column in depts[tile]:
                if column in Settings.BANNED_COLUMNS:
                    continue

                worker = Worker(good_table_name(tile), column, depts[tile][column])

                proc = multiprocessing.Process(target = worker.start_working)
                self.logger.starting_worker(tile, column)

                workers.append((worker, proc))
                proc.start()
                time.sleep(5)

                while len(workers) >= 4:
                    for w, p in workers:
                        p.join(timeout = 0)
                        if not p.is_alive():
                            workers.remove((w, p))
                            break

        def start_ssh_job(self, host, tile):
            timeout = 60 * 60 * 2

            process = subprocess.Popen(['ssh', host, '"python3 emag-scraping/process.py {}"'.format(tile)])
            time.sleep(5)

            if process.poll() is not None:
                print('Done {}'.format(tile))
