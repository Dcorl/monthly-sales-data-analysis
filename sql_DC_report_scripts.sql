-- Author: Destiny Corley
-- Date: 07/03/2025
-- Project: Sales Data Analysis
-- Description: PostgreSQL queries to create and populate tables,
--              generate summary data, and automate reporting 
--              for analyzing product performance, revenue by category,
--              and effectiveness of sales campaigns.


--Create Detailed Table
CREATE TABLE detailed_table(
	product_id SMALLINT,
	product_name TEXT,
	product_category TEXT,
	quantity SMALLINT,
	item_total NUMERIC,
	sales_date DATE,
	channel TEXT,
	channel_campaigns TEXT
);

--Create Popluate Detailed Table funculon
CREATE OR REPLACE FUNCTION populate_detailed()
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
	INSERT INTO detailed_table(
	product_id, product_name, product_category, quantity, item_total, sales_date, channel, channel_campaigns
	)
	SELECT 
	p.product_id, 
	p.product_name,
	p.category,
	s.quantity,
	s.item_total,
	s.sale_date,
	s.channel,
	s.channel_campaigns
	
	FROM sales s
	JOIN products p ON s.product_id = p.product_id
	ORDER BY s.sale_date ASC;

	EXCEPTION
	WHEN others THEN
		RAISE NOTICE 'Error in populate_detailed(): %', SQLERRM;
END;
$$;

-- Call Detailed Table Function
SELECT populate_detailed();

-- Displaying the Detailed Table
SELECT *
FROM detailed_table;

-- Creating Category Revenue Summary Table
CREATE TABLE category_revenue_summary(
	category TEXT PRIMARY KEY,
	quantity SMALLINT,
	total NUMERIC
);

-- Display Category Revenue Summary Table
SELECT *
FROM category_revenue_summary;

-- Function to Populate Category Revenue Table
CREATE OR REPLACE FUNCTION populate_category_summary()
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN

	TRUNCATE category_revenue_summary;
	
	INSERT INTO category_revenue_summary(
	category,
	quantity,
	total
	)
	SELECT
		product_category,
		SUM(quantity),
		SUM(item_total) AS Total
	FROM detailed_table
	GROUP BY product_category
	ORDER BY Total DESC;

	EXCEPTION
	WHEN others THEN
		RAISE NOTICE 'Error in populate_category_summary(): %', SQLERRM;
END;
$$;

-- Call Category Summary Table Function
SELECT populate_category_summary()

-- Display Category Table
SELECT *
FROM category_revenue_summary;

-- Create Product Summary Table
CREATE TABLE product_summary(
	product_id SMALLINT,
	product_name TEXT,
	quantity INTEGER,
	total NUMERIC,
	PRIMARY KEY (product_id)
);

-- Display Product Summary Table
SELECT *
FROM product_summary;

-- Product Summary Populate Function
CREATE OR REPLACE FUNCTION populate_product_summary()
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN

	TRUNCATE product_summary;

	INSERT INTO product_summary(
	product_id,
	product_name,
	quantity,
	total
	)
	SELECT
		product_id,
		product_name,
		SUM(quantity),
		SUM(item_total) AS Total
	FROM detailed_table
	GROUP BY product_id, product_name
	ORDER BY Total DESC;

	EXCEPTION
	WHEN others THEN
		RAISE NOTICE 'Error in populate_product_summary(): %', SQLERRM;
END;
$$;

-- Call Product Summary Table Function
SELECT populate_product_summary();

--Create Campaign Summary Table
CREATE TABLE campaign_summary(
	channel TEXT,
	campaign TEXT,
	quantity INTEGER,
	total NUMERIC,
	PRIMARY KEY (channel, campaign)
);

-- Display Campaign Summary Table
SELECT *
FROM campaign_summary;

-- Campaign Summary Populate Function
CREATE OR REPLACE FUNCTION populate_campaign_summary()
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN

	TRUNCATE campaign_summary;

	INSERT INTO campaign_summary(
	channel,
	campaign,
	quantity,
	total
	)
	SELECT
		channel,
		channel_campaigns,
		SUM(quantity),
		SUM(item_total) AS Total
	FROM detailed_table
	GROUP BY channel, channel_campaigns
	ORDER BY Total DESC;

	EXCEPTION
	WHEN others THEN
		RAISE NOTICE 'Error in populate_campaign_summary(): %', SQLERRM;
END;
$$;

-- Call Campaign Summary Function
SELECT populate_campaign_summary();

--Update Summary Function

CREATE OR REPLACE FUNCTION update_summaries_function()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN
  PERFORM populate_product_summary();
  PERFORM populate_category_summary();
  PERFORM populate_campaign_summary();
  RETURN NULL;
END;
$$;

--Creating Trigger To Update Summaries

CREATE TRIGGER update_summaries
AFTER INSERT OR UPDATE OR DELETE
ON detailed_table
FOR EACH STATEMENT
EXECUTE PROCEDURE update_summaries_function();

-- Refresh Tables Procedure
CREATE OR REPLACE PROCEDURE refresh_tables()
LANGUAGE plpgsql
AS
$$
BEGIN
  EXECUTE '
    CREATE TABLE IF NOT EXISTS detailed_table(
      product_id SMALLINT,
      product_name TEXT,
      product_category TEXT,
      quantity SMALLINT,
      item_total NUMERIC,
      sales_date DATE,
      channel TEXT,
      channel_campaigns TEXT,
      PRIMARY KEY (product_id, sales_date, channel, channel_campaigns)
    )
  ';
  
  EXECUTE '
    CREATE TABLE IF NOT EXISTS category_revenue_summary(
      category TEXT PRIMARY KEY,
      quantity SMALLINT,
      total NUMERIC
    )
  ';
  
  EXECUTE '
    CREATE TABLE IF NOT EXISTS product_summary(
      product_id SMALLINT PRIMARY KEY,
      product_name TEXT,
      quantity INTEGER,
      total NUMERIC
    )
  ';
  
  EXECUTE '
    CREATE TABLE IF NOT EXISTS campaign_summary(
      channel TEXT,
      campaign TEXT,
      quantity INTEGER,
      total NUMERIC,
      PRIMARY KEY (channel, campaign)
    )
  ';

  -- Clear Existing Data
  TRUNCATE detailed_table;
  TRUNCATE category_revenue_summary;
  TRUNCATE product_summary;
  TRUNCATE campaign_summary;

  -- Populate Tables with Functions
  PERFORM populate_detailed();
  PERFORM populate_product_summary();
  PERFORM populate_category_summary();
  PERFORM populate_campaign_summary();
END;
$$;

-- Call Refresh Table Functions
CALL refresh_tables ()

-- Drop Tables If needed!
DROP TABLE detailed_table;
DROP TABLE category_revenue_summary;
DROP TABLE product_summary;
DROP TABLE campaign_summary;
