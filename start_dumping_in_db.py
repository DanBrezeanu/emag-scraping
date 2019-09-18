import sqlite3
from testing import all_prods_in_url
from cats import get_dict_of_all
from selenium import webdriver

def make_good_name(name):
    final = ''
    for i in name:
        if i.isalpha():
            final += i
        else:
            break

    return final

if __name__ == '__main__':
    conn = sqlite3.connect('db.sqlite3')
    c = conn.cursor()
    driver = webdriver.Firefox()

    depts = get_dict_of_all(driver)
    create_table_query = 'CREATE TABLE {} (title text, link text, oldprice real, newprice real, image text)'

    for maj_dep_key in depts:
        try:
            c.execute(create_table_query.format(make_good_name(maj_dep_key)))
        except:
            pass

        for min_dep_key in depts[maj_dep_key]:
            for pair in depts[maj_dep_key][min_dep_key]:
                try:
                    prods = all_prods_in_url(pair[1], driver)
                except:
                    print('Failed at pair {}'.format(pair))

                for prod in prods:
                    query = """INSERT INTO {} VALUES ('{}','{}', {}, {}, '{}')""".format(
                        make_good_name(maj_dep_key), prod.title.replace("'","''"), prod.link, 0 if prod.old_price is None else prod.old_price, prod.new_price, prod.image
                    )

                    try:
                        c.execute(query)
                    except sqlite3.OperationalError as e:
                        print(str(e))
                        print(query)

                    conn.commit()
