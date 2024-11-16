CREATE OR REPLACE DATABASE airbnb_elt;
CREATE OR REPLACE SCHEMA airbnb_elt.staging;

-- Create a storage integration object
create or replace storage integration s3_airbnb_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::418295713957:role/snowflake-dbt-role' -- Put your arn data from AWS.
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3-snowflake-airbnb-elt/raw/') --Put your S3 Location
  COMMENT = 'Please note that access was only given to the CSV files' ;
-- Using the DESC command to show the options in the new storage integration
DESC integration s3_airbnb_integration;

CREATE OR REPLACE file format AIRBNB_ELT.STAGING.csv_fileformat
	type = 'csv'
	field_delimiter = ','
	skip_header = 1
	null_if = ('NULL', 'null')
	empty_field_as_null = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--**********************************************************************

CREATE OR REPLACE stage airbnb_ELT.staging.reviews_folder
	URL = 's3://s3-snowflake-airbnb-elt/raw/REVIEWS_FOLDER/'
	STORAGE_INTEGRATION = s3_airbnb_integration
	FILE_FORMAT = airbnb_ELT.staging.csv_fileformat;

LIST @airbnb_ELT.staging.reviews_folder; -- See the files in S3 connection

-- Create Raw table reviews 

DROP TABLE AIRBNB_ELT.STAGING.reviews;
CREATE TABLE IF NOT EXISTS AIRBNB_ELT.STAGING.reviews(
	listing_id int,
	-- id int,
	date varchar(30)
	-- reviewer_id int,
	-- reviewer_name varchar(60),
	-- comments string
	
);
SELECT * FROM AIRBNB_ELT.STAGING.reviews;

--****************************************

CREATE OR REPLACE stage airbnb_elt.staging.listings_folder
	URL = 's3://s3-snowflake-airbnb-elt/raw/listings/'
	STORAGE_INTEGRATION = s3_airbnb_integration
	FILE_FORMAT = airbnb_elt.staging.csv_fileformat;

LIST @airbnb_elt.staging.listings_folder;

drop table airbnb_elt.staging.listings;
CREATE TABLE IF NOT EXISTS airbnb_elt.staging.listings(
	id int,
    name string,
    host_id int,
    host_name string,
    neighbourhood_group varchar(45),
    neighbourhood varchar(45),
    latitude varchar(100),
    longitude varchar(100),
    room_type varchar(30),
    price varchar(20),
    minimum_nights int,
    number_of_reviews varchar(30),
    last_review varchar(100),
    reviews_per_month varchar(30),
    calculated_host_listings_count varchar(30),
    availability_365 varchar(30),
    number_of_reviews_ltm varchar(100),
    license varchar(100)
);