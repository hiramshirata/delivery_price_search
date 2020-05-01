import asyncio
import json
import requests
import os
import pickle

from concurrent.futures import ThreadPoolExecutor

# Add upper folder to path and import utils
import sys
sys.path.append("..")
from utils.location import get_coordinates_from_CEP

ACCESS_KEY = '69f181d5-0046-4221-b7b2-deef62bd60d5'
SECRET_KEY = '9ef4fb4f-7a1d-4e0d-a9b1-9b82873297d8'
# CEP default
CEP = '05414901'
# Porcentagem máxima que o preço do produto estará comparado com a média em outras lojas
percentage_under = 40.0
# Preço máximo de produtos a se verificar apenas por preço
max_price = 0.5
# Flag indicando se quer comparar com outras lojas
compare_prices = True

class Product(object):
    def __init__(self,product,store_id, store_name, subcorridor_name):
        if product.get('min_quantity_in_grams'):
            arg = 'balance_price'
        else:
            arg = 'price'
        self.arg = arg
        self.name = product.get('name')
        self.price = product.get(arg)
        self.product_id = product.get('product_id')
        self.store_id = store_id
        self.store_name = store_name
        self.subcorridor_name = subcorridor_name
        self.ean = product.get('ean') # código de barras

    def __hash__(self):
        return hash((self.product_id, self.store_id))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.product_id == other.product_id and self.store_id == other.store_id

def get_stores(location_lat, location_lon):
    stores_request = requests.get(
        "https://www.rappi.com.br/api-services/api/base-crack/principal?"+
        "lat="+str(location_lat)+
        "&lng="+str(location_lon)+
        "&device=2"
    )
    stores = []
    for market in stores_request.json():
        for suboptions in market.get('suboptions'):
            stores += [(st.get('store_id'), suboptions.get('name')) for st in suboptions.get('stores')]
    return set(stores)

def get_subcorridors(store_id):
    store_request = requests.get(
        'https://services.rappi.com.br/windu/corridors/sub_corridors/store/'+
        str(store_id)
    )
    if store_request.status_code != 200:
        return []
    corridors = store_request.json()
    sub_corridors = []
    for corridor in corridors:
        sub_corridors += [(sc['id'],sc['name']) for sc in corridor['sub_corridors']]
    return set(sub_corridors)

def get_subcorridor_products(store_id, subcorridor_id):
    include_stock_out = 'true'
    limit = 999
    subcorridor_request = requests.get(
        "https://services.rappi.com.br/api/subcorridor_sections/products?"+
        "store_id="+str(store_id)+
        "&subcorridor_id="+str(subcorridor_id)+
        "&include_stock_out="+include_stock_out+
        "&limit="+str(limit)+
        "&next_id=1"
    )
    products = subcorridor_request.json()['results'][0]['products']
    return products

def is_price_below_max(product, max_price):
    price = product.price
    if price < max_price:
        return True
    return False

def is_price_lower_comparison(product, percentage_under, prices_db):
    product_id = product.product_id
    arg = product.arg
    price = product.price
    if product_id not in prices_db.keys():
        try:
            request_all_prices = requests.get(
                'http://v2.rappi.com.br/api/products/'+str(product_id)
            )
            if request_all_prices.status_code != 200:
                print('Erro na request ',request_all_prices.status_code)
                return False
            stores = request_all_prices.json().get('stores')
            # "Limbo and car_wash are two types with strange prices"
            prices_for_db = {
            st.get('pivot').get('store_id') : {'price' :float(st.get('pivot').get(arg))} # podemos adicionar mais infos
            for st in stores \
            if st.get('pivot').get(arg) and \
            st.get('type') not in ['car_wash','limbo'] and \
            float(st.get('pivot').get(arg)) != 999
            }
            prices_db[product_id] = prices_for_db
        except Exception as e:
            print(e, product_id)
            return False                
    prices = [
        value['price'] for key,value in prices_db[product_id].items()
    ]
    if prices:
        avg_price = sum(prices)/len(prices)
        if max(prices) < 3*avg_price and price/avg_price < percentage_under/100:
            return True
        

def append_to_print_product(discount_products, product):
    discount_products.append((
        product.name,
        product.price,
        product.store_name,
        "https://www.rappi.com.br/product/%s_%s" % \
            (product.store_id, product.product_id ),
        product.subcorridor_name,
    ))

def compare_and_print_product(product, percentage_under, max_price, prices_db, discount_products):
    if compare_prices and is_price_lower_comparison(product, percentage_under, prices_db):
        append_to_print_product(discount_products, product)
    elif is_price_below_max(product, max_price):
        append_to_print_product(discount_products, product)

def fetch_and_add_to_products(subcorridor_id, products, store_id, store_name, subcorridor_name):
    sub_products = get_subcorridor_products(store_id, subcorridor_id)
    products.update([Product(product, store_id, store_name, subcorridor_name) for product in sub_products])

def main():
    cep = input('Digite o CEP: ')
    lat, lon = get_coordinates_from_CEP(cep if cep else CEP)
    stores = get_stores(lat, lon)
    storenames = dict((id, name) for id, name in stores)
    products = set()
    fileDir = os.path.dirname(os.path.realpath('__file__'))
    prices_file = os.path.join(fileDir, 'pickle/product_prices')
    with open(prices_file, 'rb') as entrada:
        prices_db = pickle.load(entrada)
    print('Verificando lojas')
    for i,store in enumerate(stores):
        print('Loja %d de %d' % (i, len(stores)))
        subcorridors = get_subcorridors(int(store[0]))
        store_name = store[1]
        with ThreadPoolExecutor(max_workers=100) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    fetch_and_add_to_products,
                    *(subcorridor[0], products, store[0], store_name, subcorridor[1]) # Allows us to pass in multiple arguments to `fetch`
                )
                for subcorridor in subcorridors
            ]
            asyncio.gather(*tasks)
    print('Todas as lojas agrupadas, verificando produtos...')
    discount_products = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        # Initialize the event loop        
        loop = asyncio.get_event_loop()
        # Use list comprehension to create a list of
        # tasks to complete. The executor will run the `fetch`
        # function for each csv in the csvs_to_fetch list
        tasks = [
            loop.run_in_executor(
                executor,
                compare_and_print_product,
                *(product, percentage_under, max_price, prices_db, discount_products) # Allows us to pass in multiple arguments to `fetch`
            )
            for product in products
        ]
        
        # Initializes the tasks to run and awaits their results
        asyncio.gather(*tasks)
    with open(prices_file, 'wb') as output:
        pickle.dump(prices_db, output)
    with open('precos.csv', 'w+', encoding='utf8') as print_discounts:
        print_discounts.write("Produto;Preço;Loja;Link;Corredor\n")
        for line in discount_products:
            print_discounts.write(";".join([str(element) for element in line])+'\n')

main()