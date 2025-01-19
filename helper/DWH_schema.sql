-- CREATE TABLE FOR LOAD THE DATA

-- Product data table
CREATE TABLE marketing_data (
   	product_id SERIAL PRIMARY KEY,
    product_code VARCHAR,
	product_name TEXT not null,
    availability BOOLEAN,
    is_sale BOOLEAN,
    condition VARCHAR(25),
    price_currency VARCHAR(3),
    avg_price FLOAT,
    brand VARCHAR,
    merchant VARCHAR,
    main_category VARCHAR,
    sub_category VARCHAR,
    asins VARCHAR,
    image TEXT,
    link TEXT,
    latest_date DATE,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Sales data table
CREATE TABLE sales_data (
	sales_id SERIAL PRIMARY KEY,
	product_name TEXT not null,
	actual_price NUMERIC(10,2),
	discount_price NUMERIC(10,2),
 	price_currency VARCHAR(3),
    main_category VARCHAR,
	sub_category VARCHAR,
	ratings NUMERIC,
	no_of_ratings INT,
	image TEXT,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- News data table
CREATE TABLE news_data (
	scraped_id SERIAL PRIMARY KEY,
	news_title TEXT not null,
	author TEXT,
	article TEXT,
	news_created TIMESTAMP,
    scrapped_at TIMESTAMP,
	image TEXT,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);