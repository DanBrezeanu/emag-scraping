from bs4 import BeautifulSoup


def get_dict_of_all(driver):
    url = 'https://www.emag.ro/all-departments'
    driver.get(url)
    root_url = 'https://www.emag.ro'
    prods = {}
    n_prods = 0

    innerHTML = driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
    soup = BeautifulSoup(innerHTML, 'html.parser')

    departments_tiles = soup.findAll('div', {'id': 'departments-page'})[0].findAll('div', {'id': 'department-expanded'})
    for dept in departments_tiles:
        n_prods += int(dept.findAll('h3', recursive = False)[0].findAll('span')[0].contents[0].strip().replace('produse', '').replace('de', '').replace(' ', '')[1:-1])
        tile_name = dept.findAll('h3', recursive = False)[0].contents[1].strip()
        prods[tile_name] = {}
        cols = dept.findAll('ul')[0].findAll('li', recursive = False)
        for col in cols:
            col_name = None
            elems = col.findAll('ul')[0].findAll('li')
            col_name = col.findAll('a', recursive = False)[0].findAll('h4')[0].contents[0].strip()
            prods[tile_name][col_name] = []
            for elem in elems:
                anchor = elem.findAll('a')[0]
                name = anchor.findAll('h5')[0].contents[0].strip()
                url = root_url + '/'.join(anchor['href'].strip().split('/')[:-1])
                prods[tile_name][col_name].append((name, url))
    return prods
