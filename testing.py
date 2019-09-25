from bs4 import BeautifulSoup
import re
import requests
from utils import Logger



class Product:
    def __init__(self, link, title, old_price, new_price, image):
        self.title = title
        self.link = link
        self.old_price = old_price
        self.new_price = new_price
        self.image = image

    def __str__(self):
        return 'Title: {} \n Old_price: {} \n New_price: {} \n Image: {} \n'.format(
            self.title, self.old_price, self.new_price, self.image
        )

class WebScraper:
    def __init__(self):
        self.logger = Logger()

    def get_all_departments(self):
        url = 'https://www.emag.ro/all-departments'
        request = requests.get(url)
        root_url = 'https://www.emag.ro'
        prods = {}
        n_prods = 0

        soup = BeautifulSoup(request.text, 'html.parser')

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
                    url = root_url + '/'.join(anchor['href'].strip().split('/')[:-1]) + '/sort-priceasc'
                    prods[tile_name][col_name].append((name, url))
        return prods

    def get_total_products(self, url):
        while True:
            try:
                request = requests.get(url + '/c')
                soup = BeautifulSoup(request.text, 'html.parser')

                total = int(soup.findAll('div', {'class' : 'page-container'})[0]\
                            .findAll('div', {'class' : 'listing-panel'})[1]\
                            .findAll('div', {'class' : 'listing-panel-footer'})[0]\
                            .findAll('div', {'class' : 'row'})[0]\
                            .findAll('div', recursive = False)[1]\
                            .findAll('p')[0]\
                            .findAll('strong')[1].contents[0])
                self.logger.total_products(url, total)
                return total
            except IndexError as e:
                self.logger.failed_total_number_products(url.split('/')[-2])

    def __find_items(self, response):
        soup = BeautifulSoup(response, 'html.parser')

        card_items = soup.findAll('div', {'class' : 'page-container'})[0]\
                    .findAll('div', {'id' : 'card_grid'})[0]\
                    .findAll('div', {'class' : 'card-item'})


        items = list(map(lambda item: item.findAll('div', {'class': 'card'})[0].findAll('div', {'class' : 'card-section-wrapper'})[0], card_items))
        return items

    def __extract_image(self, item):
        imagesrc = item.findAll('div', {'class': 'card-section-top'})[0]\
                       .findAll('div', {'class': 'card-heading'})[0]\
                       .findAll('a', {'class' : 'thumbnail-wrapper'})[0]\
                       .findAll('div', {'class': 'thumbnail'})[0]\
                       .findAll('img', {'class': 'lozad'})[0]['src']
        return imagesrc

    def __extract_title(self, item):
        title = item.findAll('div', {'class': 'card-section-mid'})[0]\
                    .findAll('h2', {'class': 'card-body'})[0]\
                    .findAll('a', {'class' : 'product-title'})[0].contents[0].strip()
        return title

    def __extract_link(self, item):
        link = item.findAll('div', {'class': 'card-section-mid'})[0]\
                    .findAll('h2', {'class': 'card-body'})[0]\
                    .findAll('a', {'class' : 'product-title'})[0]['href']
        return link

    def __extract_prices(self, item):
        price_card = item.findAll('div', {'class': 'card-section-btm'})[0]\
                         .findAll('div', {'class': 'card-body'})[1]

        oprice = None
        try:
            old_price = price_card.findAll('p', {'class': 'product-old-price'})[0]
            oprice = float(old_price.findAll('s')[0].contents[0].strip().replace('.', '')) + float(old_price.findAll('sup')[0].contents[0].strip()) / 100
        except:
            pass

        new_price = price_card.findAll('p', {'class': 'product-new-price'})[0]
        nprice = float(new_price.find(text = re.compile('[0-9]$')).strip().replace('.', '')) + float(new_price.findAll('sup')[0].contents[0].strip()) / 100

        return oprice, nprice

    def all_prods_in_url(self, base_url):
        page = 1
        f = open('links.txt', 'a')

        while True:
            products = []
            url = base_url + '/p{}/c'.format(page)
            f.write(url+'\n')
            try:
                request = requests.get(url)
                if request.url != url and page != 1:
                    self.logger.finished_products(url)
                    break

                items = self.__find_items(request.text)
                for item in items:
                    products.append(Product(self.__extract_link(item),
                                            self.__extract_title(item),
                                            *self.__extract_prices(item),
                                            self.__extract_prices(item)))

                _ = (yield page, products)
                page += 1
            except IndexError as e:
                self.logger.failed_to_load_products(url, request.status_code, e)
