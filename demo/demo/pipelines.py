 # Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class DemoPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ##Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if  field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        ##Category & product Type ---> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key]= value.lower()

        ## Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key]= float(value)

        ##Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability']= 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability']= int(availability_array[0])

        ##Review --> convert string to number 
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews']= int(num_reviews_string)

        ## Stars --> convert text to number 
        stars_string= adapter.get('stars')
        #split_stars_array = stars_string.split('')
        stars_text_value= stars_string.lower()
        if stars_text_value == 'zero':
            adapter['stars'] = 0
        elif stars_text_value == 'one':
            adapter['stars'] = 1
        elif stars_text_value == 'two':
            adapter['stars'] = 2
        elif stars_text_value == 'three':
            adapter['stars'] = 3
        elif stars_text_value == 'four':
            adapter['stars'] = 4
        elif stars_text_value == 'five':
            adapter['stars'] = 5
        else:
            # Handle the case where the textual representation is not recognized
            adapter['stars'] = None

        return item


import psycopg2

class SaveToPostgreSQLPipeline:
    def __init__(self):
        self.db_params = {
            'dbname': 'books',
            'user': 'admin',
            'password': 'admin',
            'host': 'localhost',
            'port': '5432',
        }

        # Establish a connection to the PostgreSQL database
        self.connection = psycopg2.connect(**self.db_params)

        # Create a cursor object to interact with the database
        self.cursor = self.connection.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            url VARCHAR(255),
            title VARCHAR(255),
            upc VARCHAR(50),
            product_type VARCHAR(50),
            price_excl_tax NUMERIC,
            price_incl_tax NUMERIC,
            tax NUMERIC,
            availability VARCHAR(50),
            num_reviews INTEGER,
            stars VARCHAR(20),
            category VARCHAR(100),
            description TEXT,
            price NUMERIC
        );
        """)
    def process_item(self, item, spider):
        data = (
            item['url'],
            item['title'],
            item['upc'], 
            item['product_type'],
            item['price_excl_tax'],
            item['price_incl_tax'],
            item['tax'],
            item['availability'],
            item['num_reviews'],
            item['stars'],
            item['category'],
            str(item['description'][0]),
            item['price']
        )

        # Execute the parameterized query
        try:
            # Execute the parameterized query
            self.cursor.execute("""
                INSERT INTO books (
                    url, title, upc, product_type, price_excl_tax, price_incl_tax,
                    tax, availability, num_reviews, stars, category, description, price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, data)
            self.connection.commit()
        except Exception as e:
            # Handle exceptions (e.g., log the error)
            spider.log(f"Error inserting item into database: {e}")

        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()


