import scrapy
import time
import re
from .constants.apteka_otsklada import *


class AptekaOtSkladaRuSpider(scrapy.Spider):
    name = 'apteka_ot_sklada_ru'
    allowed_domains = ['apteka-ot-sklada.ru']
    start_urls = [
        'https://apteka-ot-sklada.ru/catalog/kosmetika/sredstva-dlya-tela/krem-dlya-tela_-loson_-balzam',
        # 'https://apteka-ot-sklada.ru/catalog/dieticheskoe-pitanie_-napitki/travy_-chai/travy',
        # 'https://apteka-ot-sklada.ru/catalog/perevyazochnye-sredstva/leykoplastyri/bakteritsidnye-plastyri'
    ]

    cookies = {  # для города Томск
        'city': '92'
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, cookies=self.cookies, callback=self.parse_pages)

    def parse_pages(self, response):
        page_prefix = "?start="
        last_page = response.xpath(XPATH_LASTPAGE_NUMBER).get()
        last_page = int(last_page.split('=')[1])
        url = response.url
        for page_count in range(0, last_page + 1, 12):
            url_page = f'{url}{page_prefix}{page_count}'
            yield scrapy.Request(url=url_page, cookies=self.cookies, callback=self.parse_category_page)

    def parse_category_page(self, response):
        urls = response.xpath(XPATH_URLS).getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url, cookies=self.cookies, callback=self.parse)

    def get_price_data(self, response):
        prices = response.xpath(XPATH_PRICES).getall()
        prices = [price.strip() for price in prices]
        prices = [price.strip(' ₽') for price in prices if price]

        if len(prices) > 1:
            current_price = prices[0]
        else:
            current_price = ''.join(prices)
        try:
            current_price = float(current_price)
        except ValueError:
            current_price = 0.0

        if len(prices) > 1:
            original_price = prices[1]
        else:
            original_price = current_price
        try:
            original_price = float(original_price)
        except ValueError:
            original_price = current_price

        try:
            sales = int(100 - (current_price / original_price * 100))
        except ZeroDivisionError:
            sales = 0
        sales_tag = ''
        sales_tag = f"Скидка {sales}%" if sales > 0 else sales_tag
        price_data = {"current": current_price, "original": original_price, "sale_tag": sales_tag}
        return price_data

    def get_stock(self, response):
        not_stock = response.xpath(XPATH_STOCK).getall()
        # у товаров, которые отсутствуют в продаже, на странице появляется элемент с текстом "Временно нет на складе"
        # у товаров в наличии ничего подобного нет
        if not_stock:
            stock = False
        else:
            stock = True
        count = 1 if stock else 0
        stock = {"in_stock": stock, "count": count}
        return stock

    def get_metadata(self, response):
        description = response.xpath(XPATH_DESCRIPTION).getall()
        description = [element.replace('</h3>', ':').replace('::', ':').replace('.:', ':') for element in description]
        tags_re = re.compile(r"<[^>]+>")

        def remove_tags(text):
            return tags_re.sub('', text)

        all_metadata = response.xpath(XPATH_METADATA).getall()
        meta_dict = {}
        dict_key = None

        for element in all_metadata:
            if ("</strong>" in element or "</h2>" in element or "</h3>" in element) and len(element) < 30:
                dict_key = remove_tags(element)
                meta_dict[dict_key] = []
            elif dict_key:
                meta_dict[dict_key].append(remove_tags(element))
            else:
                meta_dict = {}

        metadata = {keys: "".join(values) for keys, values in meta_dict.items()}
        description = [remove_tags(element) for element in description if element]
        description = [element.strip() for element in description if element]
        description = "".join(description).strip()
        metadata['__description'] = description

        country = response.xpath(XPATH_COUNTRY).get('')
        country = country.strip()
        metadata['СТРАНА ПРОИЗВОДИТЕЛЬ'] = country

        return metadata

    def parse(self, response):
        main_image = response.xpath(XPATH_IMAGE).get()
        main_image = response.urljoin(main_image)
        brand = response.xpath(XPATH_BRAND).get('')
        brand = brand.strip()
        title = response.xpath(XPATH_TITLE).get('').strip()
        rpc = response.xpath(XPATH_RPC).get()
        sections = response.xpath(XPATH_SECTION).getall()
        sections = [sect.strip() for sect in sections]
        if sections:
            sections = sections[2:]
        marketing_tag = response.xpath(XPATH_MARKETING_TAG).get("")
        marketing_tag = marketing_tag.strip()

        item = {
            "timestamp": int(time.time()),  # Текущее время в формате timestamp
            "RPC": rpc,  # {str} Уникальный код товара
            "url": response.url,  # {str} Ссылка на страницу товара
            "title": title,  # {str} Заголовок/название товара (если в карточке товара указан цвет или объем, необходимо добавить их в title в формате: "{название}, {цвет}")
            "marketing_tags": [marketing_tag],  # {list of str} Список тегов, например: ['Популярный', 'Акция', 'Подарок'], если тэг представлен в виде изображения собирать его ненужно
            "brand": brand,  # {str} Бренд товара
            "section": sections,  # {list of str} Иерархия разделов, например: ['Игрушки', 'Развивающие и интерактивные игрушки', 'Интерактивные игрушки']
            "price_data": self.get_price_data(response),
            "stock": self.get_stock(response),
            "assets": {
                "main_image": main_image,  # {str} Ссылка на основное изображение товара
                "set_images": [main_image],  # {list of str} Список больших изображений товара
                "view360": [],  # {list of str}
                "video": []  # {list of str}
            },
            "metadata": self.get_metadata(response),
            "variants": 0,  # {int} Кол-во вариантов у товара в карточке
                            # (За вариант считать только цвет или объем/масса. Размер у одежды или обуви вариантами не считаются)
        }
        yield item
