import sqlite3
from testing import all_prods_in_url, get_total_products
from cats import get_dict_of_all
import multiprocessing
import time

def commit_queries(db_conn, *args):
    while True:
        try:
            db_conn.commit()
            break
        except sqlite3.OperationalError:
            print('Failed to commit: {}, {}. Trying again'.format(*args), flush = True)

def make_good_name(name):
    final = ''
    for i in name:
        if i.isalpha():
            final += i
        else:
            break

    return final

def check_progress():
    import datetime
    db_conn = sqlite3.connect('db.sqlite3')
    c = db_conn.cursor()

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
    db_conn.close()

def insert_into_db(column, db_conn, column_name, table, lock):
    c = db_conn.cursor()
    update_query = """UPDATE progress SET page = {}, products = {}, done = {} WHERE category = '{}'"""

    for pair in column:
        pair_total_products = get_total_products(pair[1])

        _query = """INSERT INTO progress VALUES ('{}', 0, 0, 0)""".format(pair[0])
        print('will exec {}'.format(_query))
        lock.acquire()
        c.execute(_query)
        commit_queries(db_conn, 'progress', 'initial')
        lock.release()

        n_prods = 0
        coroutine = all_prods_in_url(pair[1], lock)

        while True:
            try:
                page, prods = coroutine.send(None)
            except StopIteration:
                break

            n_prods += len(prods)

            lock.acquire()
            c.execute(update_query.format(page, n_prods, (0 if n_prods < pair_total_products else 1), pair[0]))
            conn.commit()
            lock.release()

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

                try:
                    lock.acquire()
                    c.execute(query)
                    lock.release()
                except sqlite3.OperationalError as e:
                    lock.release()
                    print(str(e))
                    print(query, flush = True)
                    

            commit_queries(db_conn, pair[1].split('/')[-2], page)

            print('Comitted {} entries from {}, page {}'.format(len(prods), pair[1].split('/')[-2], page), flush = True)


if __name__ == '__main__':
    lock = multiprocessing.Lock()
    conn = sqlite3.connect('db.sqlite3')
    c = conn.cursor()

    depts = get_dict_of_all()
    create_table_query = 'CREATE TABLE {} (title text, link text, oldprice real, newprice real, column_name text, category text)'
    create_table_progress = 'CREATE TABLE progress (category text, page integer, products integer, done integer)'

    c.execute(create_table_progress)
    conn.commit()

    for maj_dep_key in depts:
        c.execute(create_table_query.format(make_good_name(maj_dep_key)))
        conn.commit()
        print('Created table {}'.format(make_good_name(maj_dep_key)))

        procs = []
        for min_dep_key in depts[maj_dep_key]:
            proc = multiprocessing.Process(target=insert_into_db, args=(depts[maj_dep_key][min_dep_key],
                                                                        conn,
                                                                        min_dep_key,
                                                                        make_good_name(maj_dep_key), lock))
            proc.start()
            procs.append(proc)
            time.sleep(5)


        for p in procs:
            p.join()
