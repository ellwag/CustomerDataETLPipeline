# Customer Data ETL Pipeline

This ETL (Extract, Transform, Load) pipeline is designed to process customer data from CSV files into a PostgreSQL database. The pipeline extracts data, transforms it according to the business logic (normalization) I defined, and loads it into a normalized database schema. This project showcases the use of Python with libraries such as Pandas and SQLAlchemy to handle data processing tasks efficiently.

## Features

- **Extract**: Read data from CSV files using Pandas.
- **Transform**: Clean and transform data using Python and SQL, including:
  - Renaming columns for consistency.
  - Converting 'Yes'/'No' strings to BOOLEAN values.
  - Normalizing data into separate tables.
  - Handling missing values through averages.
- **Load**: Insert data into PostgreSQL using SQLAlchemy with upsert functionality.
- **Schema Management**: Define and create database schema using SQLAlchemy's MetaData.
- **Logging**: Track the ETL process and errors using Python's logging module.


## Database Schema

**staging_customer_data**: 
A staging table that temporarily holds raw data imported from the CSV file. The structure for this table includes:
- **customer_id (Integer, Primary Key)**: Unique identifier for the customer.
- **category (String)**: Category of the item purchased.
- **age (Integer)**: Age of the customer.
- **gender (String)**: Gender of the customer.
- **location (String)**: Location where the purchase was made or where the customer resides.
- **color (String)**: Color of the item purchased.
- **size (String)**: Size of the item purchased.
- **season (String)**: Season during which the purchase was made.
- **item_purchased (String)**: Name or identifier of the item purchased.
- **purchase_amount (Float)**: The amount of money spent on the purchase in USD.
- **review_rating (Float)**: Rating given by the customer for the purchased item.
- **subscription_status (String)**: Indicates whether the customer is subscribed to a service (likely to be transformed to a Boolean value).
- **payment_method (String)**: Method used by the customer to make the payment.
- **shipping_type (String)**: Type of shipping chosen for the delivery of the item.
- **discount_applied (String)**: Indicates whether a discount was applied to the purchase (likely to be transformed to a Boolean value).
- **promo_code_used (String)**: Indicates whether a promo code was used during the purchase (likely to be transformed to a Boolean value).
- **previous_purchase (String)**: Information on previous purchases made by the customer.
- **preferred_payment_method (String)**: Customer's preferred method of payment.
- **frequency_of_purchase (String)**: How often the customer makes purchases.

**customers**:
A table that will contain unique customer information, derived from the staging_customer_data table. 
- **customer_id (Integer, Primary Key)**: Unique identifier for the customer.
- **age (Integer)**: Age of the customer.
- **gender (String)**: Gender of the customer.
- **subscription_status (String)**: Indicates whether the customer is subscribed to a service (likely to be transformed to a Boolean value).

**products**:
A table that will contain details about the products purchased, such as name, category, size, color, and season. This table will be populated with distinct product information from the staging_customer_data table. The structure for this table includes:
- item_purchased (String): Name or identifier of the item purchased.
- category (String): Category of the item purchased.
- size (String): Size of the item purchased.
- color (String): Color of the item purchased.
- season (String): Season during which the item is typically purchased or used.


**purchases**:
A table that will hold records of each purchase, including customer ID, item purchased, purchase amount, location, and review rating. This table is created by selecting relevant columns from the staging_customer_data table and joining with other tables if normalization is required.
These tables form the core of the database schema for the ETL pipeline and will be used to store and organize the customer data effectively. The schema is designed to facilitate easy data retrieval for analysis and reporting purposes.
- customer_id (Integer): Identifier for the customer making the purchase.
- item_purchased (String): Name or identifier of the item purchased.
- purchase_amount (Float): The amount of money spent on the purchase in USD.
- location (String): Location where the purchase was made.
- review_rating (Float): Rating given by the customer for the purchased item.

## The ETL Process
The `etl` function orchestrates the entire ETL process by:
1. **Loading the configuration** from a YAML file, which includes the file path for the CSV data, database configuration, and log file path.
2. **Defining the primary keys** and the name of the staging table.
3. **Setting up the table schema** with the appropriate columns and data types.
4. **Configuring the logging** to record the ETL process and any errors that occur.
5. **Creating a SQLAlchemy engine** to connect to the PostgreSQL database using the credentials from the configuration file.
6. **Running the ETL process**, which includes:
   - Extracting data from the CSV file.
   - Creating the staging table schema in the database if it doesn't exist.
   - Loading data into the staging table with the correct column mapping.
   - **The `transform_and_load` function** governs the transformation and loading of data into the normalized tables (customers, products, purchases) and creating them if they don't exist.
     - Transforming values for fields such as `subscription_status`, `discount_applied`, and `promo_code_used` that have 'Yes'/'No' strings to BOOLEAN values.
     - Updating missing review ratings with the average rating.
7. **Handling any exceptions** that occur during the ETL process by logging the error.
8. **Disposing of the database engine connection** once the ETL process is complete or if an exception occurs.



## Configuration

Before running the ETL pipeline, ensure that you have a `config.yaml` file with the following structure:

```yaml
csv_file_path: 'path/to/your/csvfile.csv'
db_config:
  user: 'your_username'
  password: 'your_password'
  host: 'your_host'
  port: 'your_port'
  database: 'your_database'
log_file_path: 'path/to/your/logfile.log'
```
