from bs4 import BeautifulSoup
import re
import requests



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

def get_total_products(url):
    while True:
        try:
            request = requests.get(url + '/c')
            print('url: ' + url)
            soup = BeautifulSoup(request.text, 'html.parser')

            total = int(soup.findAll('div', {'class' : 'page-container'})[0]\
                        .findAll('div', {'class' : 'listing-panel'})[1]\
                        .findAll('div', {'class' : 'listing-panel-footer'})[0]\
                        .findAll('div', {'class' : 'row'})[0]\
                        .findAll('div', recursive = False)[1]\
                        .findAll('p')[0]\
                        .findAll('strong')[1].contents[0])
            return total
        except IndexError as e:
            print('Could not get total products for {}. Trying again'.format(url.split('/')[-2]))
            print(str(e))

def all_prods_in_url(base_url, lock):
    page = 1

    while True:
        products = []
        url = base_url + '/p{}/c'.format(page)

        try:
            request = requests.get(url)

            if request.url != url and page != 1:
                break

            soup = BeautifulSoup(request.text, 'html.parser')

            card_items = soup.findAll('div', {'class' : 'page-container'})[0]\
                        .findAll('div', {'id' : 'card_grid'})[0]\
                        .findAll('div', {'class' : 'card-item'})


            items = list(map(lambda item: item.findAll('div', {'class': 'card'})[0].findAll('div', {'class' : 'card-section-wrapper'})[0], card_items))

            for item in items:
                imagesrc = item.findAll('div', {'class': 'card-section-top'})[0]\
                               .findAll('div', {'class': 'card-heading'})[0]\
                               .findAll('a', {'class' : 'thumbnail-wrapper'})[0]\
                               .findAll('div', {'class': 'thumbnail'})[0]\
                               .findAll('img', {'class': 'lozad'})[0]['src']

                title = item.findAll('div', {'class': 'card-section-mid'})[0]\
                            .findAll('h2', {'class': 'card-body'})[0]\
                            .findAll('a', {'class' : 'product-title'})[0].contents[0].strip()

                link = item.findAll('div', {'class': 'card-section-mid'})[0]\
                            .findAll('h2', {'class': 'card-body'})[0]\
                            .findAll('a', {'class' : 'product-title'})[0]['href']

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
                products.append(Product(link, title, oprice, nprice, imagesrc))

            _ = (yield page, products)
            page += 1
        except IndexError as e:
            print('Couldn\'t get page {}. Trying again'.format(url), flush = True)
            print(request.status_code)
            print(str(e))
