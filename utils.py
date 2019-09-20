from enum import Enum

class Status(Enum):
    INFO = 0
    WARN = 1
    ERROR = 2


class Logger():
    def table_already_exists(self, table_name, status = Status.WARN, extra = ''):
        print('[{}]  Table {} already exists. {}'.format(status.name, table_name, extra))

    def failed_to_commit(self, *args, status = Status.ERROR):
        print('[{}] Failed to commit: {}. Trying again'.format(status.name, ', '.join(str(arg) for arg in args)), flush = True)

    def failed_to_execute(self, exception, status = Status.ERROR, query = None):
        print('[{}] Failed to execute query. \nException: {}'.format(status.name, str(exception)))
        if query is not None:
            print(query)

    def failed_total_number_products(self, *args, status = Status.WARN):
        print('[{}] Could not get total products for {}. Trying again'.format(', '.join(str(arg) for arg in args)))

    def failed_to_load_products(self, url, status_code, exception, status = Status.ERROR):
        print('[{}] Could not load products from URL {}.\nStatus code: {}\nException: {}'.format(url, status_code, exception))

def good_table_name(name):
    final = ''
    for i in name:
        if i.isalpha():
            final += i
        else:
            break

    return final
