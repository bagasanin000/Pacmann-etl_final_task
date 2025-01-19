import luigi
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup
import requests
import time
from pangres import upsert
import logging
from helper.db_connector import sales_db_engine, dwh_engine
from helper.data_validator import validation_process
from helper.dir_cleaner import clean_output

load_dotenv()

DIR_LOG = os.getenv("DIR_LOG")
DIR_DATASET = os.getenv("DIR_DATASET")
DIR_EXTRACT = os.getenv("DIR_EXTRACT")
DIR_TRANSFORM = os.getenv("DIR_TRANSFORM")
DIR_LOAD = os.getenv("DIR_LOAD")

clean_output()

# luigi task for extract marketing data
class ExtractMarketingData(luigi.Task):
    
    def requires(self):
        """
        there's no dependencies yet, so skip to the next task using pass.
        
        """
        pass
    
    def output(self):
        """
        output => extracted data.
        
        """
        return luigi.LocalTarget(f'{DIR_EXTRACT}/extract_marketing_data.csv')
        
    def run(self):
        """
        main function to read the marketing data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # read the data  
        product_data = pd.read_csv(f'{DIR_DATASET}/ElectronicsProductsPricingData.csv')
        
        # export to CSV
        product_data.to_csv(self.output().path, index=False)
        print("Marketing data has been successfully extracted.")
        
# luigi task for extract sales data       
class ExtractSalesData(luigi.Task):
    
    def requires(self):
        """
        there's no dependencies yet, so skip to the next task using pass.
        
        """
        pass
    
    def output(self):
        """
        output => extracted data.
        
        """
        return luigi.LocalTarget(f'{DIR_EXTRACT}/extract_sales_data.csv')
        
    def run(self):
        """
        main function to read the marketing data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # connect to sales database engine
        engine = sales_db_engine()
        
        query = 'SELECT * FROM amazon_sales_data'
        
        # read the sales data from the database
        extract_sales_data = pd.read_sql(sql=query, con=engine) 
        
        # export to CSV
        extract_sales_data.to_csv(self.output().path, index=False)
        print("Sales data has been successfully extracted from Dockerized database PostgreSQL.")
        

# luigi task for scraping news data      
class ExtractNewsData(luigi.Task):
    
    def requires(self):
        """
        there's no dependencies yet, so skip to the next task using pass.
        
        """
        pass
    
    def output(self):
        """
        output => extracted data.
        
        """
        return luigi.LocalTarget(f'{DIR_EXTRACT}/extract_news_data.csv')
        
    def run(self):
        """
        main function to read the marketing data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')

        print("Scraping CNN Indonesia articles...")
        url = "https://www.cnnindonesia.com/indeks"

        # check the response
        try:
            resp = requests.get(url)
            resp.raise_for_status()  
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return
        
        # create soup object
        soup = BeautifulSoup(resp.text, "html.parser")

        # scrape the urls on each articles
        raw_link = soup.find_all("a", class_ = "flex group items-center gap-4")
        
        # scrape from page 1 - 10
        links = []

        for page in range(1,6):
            resp = requests.get(f"{url}/2?page={page}")

            soup = BeautifulSoup(resp.text, "html.parser")

            raw_link = soup.find_all("a", class_ = "flex group items-center gap-4")

            # print raw_link
            for link in raw_link:
                get_link = link.get("href")
                links.append(get_link)

            time.sleep(0.5) # add delay 0.3 to avoid bot problem

        # # save as dataframe
        # raw_url_news = pd.DataFrame(links, columns = ["url"])

        # get news title, author, article content, news created
        # get the current timestamp
        current_timestamp = pd.Timestamp.now()

        # convert the Timestamp object to a formatted string
        formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        # # scrape more
        # resp2 = requests.get(get_link)
        # soup2 = BeautifulSoup(resp2.text, "html.parser")
        
        full_data = []

        for idx in range(len(links)):
            try:
                print(f"Scraping news {idx} of {len(links)}")
                # get_link = links["url"].loc[idx]
                get_link = links[idx]

                resp2 = requests.get(get_link)

                soup2 = BeautifulSoup(resp2.text, "html.parser")

                get_title = soup2.find("h1").text.strip()

                if 'FOTO:' in get_title or "SMASHOT:" in get_title:
                    continue

                get_article = soup2.find_all("p")

                final_article = ""

                for article in get_article:
                    if "ADVERTISEMENT" not in article.text and "SCROLL TO CONTINUE WITH CONTENT" not in article.text:
                        final_article += article.text + "\n"

                get_author = soup2.find("span", class_ = "text-cnn_red")
                get_author = get_author.text if get_author else None

                # there is some html tag for news dates
                if soup2.find("div", class_ = "text-cnn_grey text-sm mb-4"):
                    news_created = soup2.find("div", class_ = "text-cnn_grey text-sm mb-4").text.strip()
                elif soup2.find("div", class_ = "text-cnn_grey text-sm mb-6"):
                    news_created = soup2.find("div", class_ = "text-cnn_grey text-sm mb-6").text.strip()
                else :
                    news_created = ""
                    
                # get image url
                get_image = soup2.find("img", class_ = "w-full")
                if get_image:
                    image_url = get_image.get("src")
                    # image_name = image_url.split("/")[-1]
                    # image_name = image_name.split("?")[0]
                else:
                    image_url=None

                news_data = {
                    "news_title": get_title,
                    "author": get_author,
                    "article": final_article.strip(),
                    "news_created": news_created,
                    "scrapped_at": formatted_timestamp,
                    "image": image_url
                }

                full_data.append(news_data)

                time.sleep(0.3)

            except Exception as e:
                raise Exception(f"There's some error: {e}")
        
        news_data = pd.DataFrame(full_data)
            
        # export to CSV
        news_data.to_csv(self.output().path, index=False)

        print("News data has been successfully scraped.")
# -------------------------------------------------------------------------------------------------------------------------- #

# task to validate the extracted and scraped data
class ValidateData(luigi.Task):
    def requires(self):
        return [ExtractMarketingData(), ExtractSalesData(), ExtractNewsData()]    

    def output(self):
        pass
    
    def run(self):
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # read marketing data for validation
        marketing_data = pd.read_csv(self.input()[0].path)

        # read sales data for validation
        sales_data = pd.read_csv(self.input()[1].path)

        # read scraped news data for validation
        news_data = pd.read_csv(self.input()[2].path)

        # apply validation function can be applied here
        validation_process(sales_data, "extract_sales_data")
        validation_process(marketing_data, "extract_marketing_data")
        validation_process(news_data, "extract_news_data")

# -------------------------------------------------------------------------------------------------------------------------- #


# luigi task for extract marketing data
class TransformMarketingData(luigi.Task):
    
    def requires(self):
        """
        this section depends on ExtractMarketingData and its extracted CSV file
        
        """
        return ExtractMarketingData()
    
    def output(self):
        """
        output => transformed data.
        
        """
        return luigi.LocalTarget(f'{DIR_TRANSFORM}/transform_marketing_data.csv')
        
    def run(self):
        """
        main function to read, validate, and transform the marketing data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        print("Transforming marketing data...")
        marketing_data = pd.read_csv(self.input().path)

        # drop duplicated data
        marketing_data.drop_duplicates(inplace=True)

        # drop some columns
        marketing_data.drop(['upc', 'weight', 'prices.sourceURLs', 'ean',
                                    'keys', 'prices.shipping', 'dateAdded',
                                    'dateUpdated', 'manufacturer', 'manufacturerNumber',
                                    'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28',
                                    'Unnamed: 29', 'Unnamed: 30'], axis = 1, inplace = True)

        # rename some columns
        dict = {
            'id' : 'product_code',
            'name' : 'product_name',
            'prices.availability' : 'availability',
            'prices.condition' : 'condition',
            'prices.currency' : 'price_currency',
            'prices.dateSeen' : 'latest_date',
            'prices.isSale' : 'is_sale',
            'prices.merchant' : 'merchant',
            'primaryCategories' : 'main_category',
            'categories' : 'sub_category',
            'imageURLs' : 'image',
            'sourceURLs' : 'link'
        }
        marketing_data.rename(columns = dict, inplace= True)

        # Create avg_price column
        marketing_data['avg_price'] = marketing_data[['prices.amountMax', 'prices.amountMin']].mean(axis=1).round(2)
        marketing_data.drop(['prices.amountMax', 'prices.amountMin'], axis = 1, inplace = True)

        # map the values from availability column
        availability = marketing_data['availability']

        CONVERT_VALUES1 = {
            'Yes': True,
            'In Stock': True,
            'TRUE': True,
            'yes': True,
            'Special Order': True,
            'More on the Way': True,
            '32 available': True,
            '7 available' : True,
            'undefined': False,
            'Out of Stock': False,
            'No': False,
            'sold': False,
            'FALSE': False,
            'Retired': False
        }

        availability = availability.replace(CONVERT_VALUES1)

        # for value in availability:
        #     availability.append(CONVERT_VALUES1, inplace=True)

        # map the values from condition column
        condition_list = []
        for cond in marketing_data['condition']:
            if 'new' in cond.lower():
                temp = 'new'
            elif 'refurbished' in cond.lower():
                temp = 'refurbished'
            elif ('used' in cond.lower()) | ('pre-owned' in cond.lower()):
                temp = 'used'
            else:
                temp = 'new'
            condition_list.append(temp)

        marketing_data['condition'] = condition_list

        # map the values from condition column
        # marketing_data['main_category'].append('electronics', inplace=True)
        marketing_data['main_category'] = 'electronics'

        # cast the availability column to boolean.
        marketing_data['availability'] = marketing_data['availability'].astype(bool)

        # transform "dateSeen" column
        def get_latest_date(date_str):
            try:
                
                # Split the string value by comma
                date_list = date_str.split(',')

                # Convert all dates to a datetime
                dates = pd.to_datetime(date_list, errors='coerce')
                
                # Get the latest date and return it
                latest_date = dates.max()
                
                return latest_date
                    
            except Exception as e:
                print(f'Error: {e}')
                return None
            
        # apply the function to update the current data
        marketing_data['latest_date'] = marketing_data['latest_date'].apply(get_latest_date)

        # change the datetime format
        marketing_data['latest_date'] = marketing_data['latest_date'].dt.strftime('%Y-%m-%d')

        # cast the data type to datetime
        marketing_data['latest_date'] = marketing_data['latest_date'].astype('datetime64[ns]')

        # normalize some values to lower case
        marketing_data['product_name'] = marketing_data['product_name'].str.lower()
        marketing_data['merchant'] = marketing_data['merchant'].str.lower()
        marketing_data['brand'] = marketing_data['brand'].str.lower()
        marketing_data['sub_category'] = marketing_data['sub_category'].str.lower()

        # mapping the columns
        mapping_cols = [
            'product_code',
            'product_name',
            'availability',
            'is_sale',
            'condition',
            'price_currency',
            'avg_price',
            'brand',
            'merchant',
            'main_category',
            'sub_category',
            'asins',
            'image',
            'link',
            'latest_date'
        ]

        # update the data
        marketing_data = marketing_data[mapping_cols]

        # export to CSV
        marketing_data.to_csv(self.output().path, index=False)
        print("Marketing data has been successfully transformed.")
        
# luigi task for extract sales data       
class TransformSalesData(luigi.Task):
    
    def requires(self):
        """
        this section depends on ExtractSalesData and its extracted CSV file
        
        """
        return ExtractSalesData()
    
    def output(self):
        """
        output => transformed data.
        
        """
        return luigi.LocalTarget(f'{DIR_TRANSFORM}/transform_sales_data.csv')
        
    def run(self):
        """
        main function to read, validate, and transform the sales data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        print("Transforming marketing data...")
        sales_data = pd.read_csv(self.input().path)
        
        # drop a column
        sales_data.drop(['Unnamed: 0'], axis = 1, inplace = True)

        # rename to product_name
        sales_data.rename(columns = {'name' : 'product_name'}, inplace=True)

        # map values on main_category column
        category_map = {
                    
                    "women's clothing": "women's fashion",
                    "men's clothing": "men's fashion",
                    "men's shoes": "men's fashion",
                    "women's shoes": "women's fashion",
                    "kids' fashion": "kid's fashion",
                    'sports & fitness': 'sports & outdoor',
                    'accessories': 'jewelry & accessories',
                    'appliances': 'electronics',
                    'tv, audio & cameras': 'electronics',
                    'car & motorbike': 'automotive & motorcycle',
                    'stores': 'retail store',
                    'grocery & gourmet foods': 'food & beverages',
                    'music': 'film & music',
                    'home, kitchen, pets': 'home & kitchen',
                    "toys & baby products": "toys & baby",
                    
                    # not required to be mapped
                    'beauty & health': 'beauty & health',
                    'home & kitchen' : 'home & kitchen',
                    'bags & luggage': 'bags & luggage',
                    'pet supplies': 'pet supplies',
                    'industrial supplies': 'industrial supplies'
                }

        # update the data 
        sales_data['main_category'] = sales_data['main_category'].map(category_map)

        # normalize to lowercase
        sales_data['product_name'] = sales_data['product_name'].str.lower()
        sales_data['sub_category'] = sales_data['sub_category'].str.lower()
        
        # transform ratings column
        # remove non numeric char using a regex pattern
        # extract specific digits
        get_digit = r'(\d+\.\d+|\b[0-5]\b)'

        # extract and update the data
        sales_data['ratings'] = sales_data['ratings'].str.extract(get_digit)
                
        # cast the data type to a numeric
        sales_data['ratings'] = pd.to_numeric(sales_data['ratings'], errors='coerce')

        # transform no_of_ratings column
        # remove non-numeric char using a regex pattern and update the data
        # sales_data['no_of_ratings'] = sales_data['no_of_ratings'].str.append(r'\D', '', regex=True)
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].str.replace(r'\D', '', regex=True)
                
        # cast the data type to a numeric
        sales_data['no_of_ratings'] = pd.to_numeric(sales_data['no_of_ratings'], errors='coerce')

        # transform discount_price and actual_price columns
        # remove non numeric char using a regex and update the data
        # sales_data['actual_price'] = sales_data['actual_price'].str.append(r'\D', '', regex=True)
        # sales_data['discount_price'] = sales_data['discount_price'].str.append(r'\D', '', regex=True)
        sales_data['actual_price'] = sales_data['actual_price'].str.replace(r'\D', '', regex=True)
        sales_data['discount_price'] = sales_data['discount_price'].str.replace(r'\D', '', regex=True)

        # cast the data type to a numeric
        sales_data['actual_price'] = pd.to_numeric(sales_data['actual_price'], errors='coerce')
        sales_data['discount_price'] = pd.to_numeric(sales_data['discount_price'], errors='coerce')
        
        # from INR to USD 
        usd_rate = 0.012 # rate per Jan 2025

        # convert the values
        actual_to_usd = round(sales_data['actual_price'] * usd_rate, 2)
        disc_to_usd = round(sales_data['discount_price'] * usd_rate, 2)
                
        # update the data 
        sales_data['actual_price'] = actual_to_usd
        sales_data['discount_price'] = disc_to_usd 

        # ceate new column to store the price currency
        sales_data['price_currency'] = 'USD'

        # mapping columns order
        mapping_cols = [
            'product_name',
            'actual_price',
            'discount_price',
            'price_currency',
            'main_category',
            'sub_category',
            'ratings',
            'no_of_ratings',
            'image',
            'link'
        ]

        # udate the data
        sales_data = sales_data[mapping_cols]

        # handle missing values

        # in 'actual_price' column -> impute to median value on each category
        # group the sales data by the 'main_category' column
        sales_by_category = sales_data.groupby(by='main_category')

        # get the median value for each category
        get_median_price = sales_by_category['actual_price'].transform('median')

        # update the data
        sales_data.loc[:, 'actual_price'] = sales_data['actual_price'].fillna(get_median_price)

        # in 'dicount_price' column -> impute to 0 (no discount)
        sales_data.loc[:, 'discount_price'] = sales_data['discount_price'].fillna(0)

        # in 'no_of_ratings' column -> drop missing values
        sales_data = sales_data.dropna(subset='no_of_ratings')

        # drop duplicated data
        sales_data.drop_duplicates(inplace=True)

        # export to CSV
        sales_data.to_csv(self.output().path, index=False)
        print("Sales data has been successfully transformed.")
        
# luigi task for scraping news data      
class TransformNewsData(luigi.Task):
    
    def requires(self):
        """
        this section depends on ExtractNewsData and its extracted CSV file
        
        """
        return ExtractNewsData()
    
    def output(self):
        """
        output => extracted data.
        
        """
        return luigi.LocalTarget(f'{DIR_TRANSFORM}/transform_news_data.csv')
        
    def run(self):
        """
        main function to read, validate, and transform the scraped news data and export it as CSV file
        
        """
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        print("Transforming scraped news data...")
        news_data = pd.read_csv(self.input().path)

        # remove /n on article column
        # news_data["article"] = news_data["article"].str.append("\xa0", " ")
        news_data["article"] = news_data["article"].str.replace("\xa0", " ", regex=False)


        # cast 'news_created' and 'scrapped_at' column to datetime

        # transform news_created column
        def get_news_created(date_str):
            try:
                
                # split the string value by comma
                # print(date_str.append("WIB", "").split(",")[1])
                date_list = date_str.append("WIB", "").split(',')[1]

                # convert all dates to a datetime
                dates = pd.to_datetime(date_list, errors='coerce')
            
                # get the latest date and return it
                news_created = dates
                        
                return news_created
                    
            except Exception as e:
                print(f'Error: {e}')
                return None
            
        # apply the function to update the current data
        news_data['news_created'] = news_data['news_created'].apply(get_news_created)
        news_data['news_created'] = news_data['news_created'].astype('datetime64[ns]')

        news_data['scrapped_at'] = pd.to_datetime(news_data['scrapped_at']).dt.strftime("%Y-%m-%d")
        news_data['scrapped_at'] = news_data['scrapped_at'].astype('datetime64[ns]')
                    
        # export to CSV
        news_data.to_csv(self.output().path, index=False)
        print("Scraped news data has been successfully transformed.")

# -------------------------------------------------------------------------------------------------------------------------- #

# task to load the transformed data
class LoadData(luigi.Task):
    def requires(self):
        return [TransformMarketingData(), TransformSalesData(), TransformNewsData()]    

    def output(self):
        return [luigi.LocalTarget(f"{DIR_LOAD}/load_marketing_analysis_data.csv"),
                luigi.LocalTarget(f"{DIR_LOAD}/load_sales_analysis_data.csv"),
                luigi.LocalTarget(f"{DIR_LOAD}/load_scraped_news_data.csv")]
    
    def run(self):
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
            level = logging.INFO, 
            format = '%(asctime)s - %(levelname)s - %(message)s')

        try:
            # dwh engine connection
            engine = dwh_engine()
            
            # read the output file from each tranformation task
            load_marketing_data = pd.read_csv(self.input()[0].path)
            load_marketing_data.insert(0, 'product_id', range(0, 0 + len(load_marketing_data)))
            load_marketing_data.set_index('product_id')

            load_sales_data = pd.read_csv(self.input()[1].path)
            load_sales_data.insert(0, 'sales_id', range(0, 0 + len(load_sales_data)))
            load_sales_data.set_index('sales_id')

            load_news_data = pd.read_csv(self.input()[2].path)
            load_news_data.insert(0, 'scraped_id', range(0, 0 + len(load_news_data)))
            load_news_data.set_index('scraped_id')


            # init table name for each task
            marketing_data_table = 'marketing_data'
            sales_data_table = 'sales_data'
            news_data_table = 'news_data' 
        
            # Insert each transformed data into the database
            load_marketing_data.to_sql(name=marketing_data_table,
                                     con=engine,
                                     if_exists='replace',
                                     index=False)
            
            load_sales_data.to_sql(name=sales_data_table,
                                   con=engine,
                                   if_exists='replace',
                                   index=False)
            
            load_news_data.to_sql(name=news_data_table,
                                     con=engine,
                                     if_exists='replace',
                                     index=False)
        
        except Exception as e:
            print(f'Error: {e}')

        # save the process output
        load_marketing_data.to_csv(self.output()[0].path, index=False)
        load_sales_data.to_csv(self.output()[1].path, index=False)
        load_news_data.to_csv(self.output()[2].path, index=False)




# -------------------------------------------------------------------------------------------------------------------------- #

if __name__ == "__main__":
    luigi.build([ExtractMarketingData(),
                ExtractSalesData(),
                ExtractNewsData(),
                ValidateData(),
                TransformMarketingData(),
                TransformSalesData(),
                TransformNewsData(),
                LoadData()])