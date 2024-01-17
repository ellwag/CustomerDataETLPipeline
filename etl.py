import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, String,  MetaData, text
from sqlalchemy.dialects.postgresql import insert
import yaml
import traceback
import logging

# Load configuration
def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config

# Extract
def extract(file_path):
    return pd.read_csv(file_path)

# Load (to a staging table)
def load_to_staging(df, engine, table_schema, primary_keys):
    try:
        column_mapping = {
            'Customer ID': 'customer_id',
            'Payment Method': 'payment_method',
            'Item Purchased': 'item_purchased',
            'Size': 'size',
            'Preferred Payment Method': 'preferred_payment_method',
            'Discount Applied': 'discount_applied',
            'Color': 'color',
            'Purchase Amount (USD)': 'purchase_amount',
            'Gender': 'gender',
            'Season': 'season',
            'Shipping Type': 'shipping_type',
            'Review Rating': 'review_rating',
            'Age': 'age',
            'Frequency of Purchases': 'frequency_of_purchase',
            'Promo Code Used': 'promo_code_used',
            'Subscription Status': 'subscription_status',
            'Category': 'category',
            'Location': 'location',
            'Previous Purchases': 'previous_purchase'
        }

        # Rename the columns in the DataFrame
        df = df.rename(columns=column_mapping)
        print(df.head())
        # Convert the DataFrame to a list of dictionaries for insertion
        list_to_write = df.to_dict(orient='records')
    
        with engine.connect() as conn:
            for record in list_to_write:
                # Create an insert statement for the record
                stmt = insert(table_schema).values(**record)
                
                # On conflict with the primary key, update the record
                on_conflict_stmt = stmt.on_conflict_do_update(
                    index_elements=primary_keys,
                    set_={c.key: c for c in stmt.excluded if not c.primary_key}
                )
                conn.execute(on_conflict_stmt)
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred while executing SQL: {e}")
        # Log the full stack trace
        logging.error(traceback.format_exc())  


def execute_sql(engine, sql):
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
    except Exception as e:
        print(e)
        logging.error(f"An error occurred while executing SQL: {e}")
        # Log the full stack trace
        logging.error(traceback.format_exc()) 

def staging_database_schema(engine, staging_table_name):
    """Alter tables names for easier access"""
    metadata = MetaData()
    table = Table(staging_table_name, metadata,
        Column('customer_id', Integer, primary_key=True),
        Column('category', String),
        Column('age', Integer),
        Column('gender', String),
        Column('location', String),
        Column('color', String),
        Column('size', String),
        Column('season', String),
        Column('item_purchased', String),
        Column('purchase_amount', Float),
        Column('review_rating', Float),
        Column('subscription_status', String),
        Column('payment_method', String),
        Column('shipping_type', String),
        Column('discount_applied', String),
        Column('promo_code_used', String),
        Column('previous_purchase', String),
        Column('preferred_payment_method', String),
        Column('frequency_of_purchase', String),
    )

    # Create the table in the database if it does not exist
    metadata.create_all(engine)

def convert_yes_no_to_boolean(engine, staging_table_name):
    """Convert 'Yes'/'No' strings to BOOLEAN."""
    sql = f"""
        UPDATE {staging_table_name}
        SET subscription_status = CASE WHEN subscription_status = 'Yes' THEN TRUE ELSE FALSE END,
            discount_applied = CASE WHEN discount_applied = 'Yes' THEN TRUE ELSE FALSE END,
            promo_code_used = CASE WHEN promo_code_used = 'Yes' THEN TRUE ELSE FALSE END;
    """
    execute_sql(engine, sql)
"""
MAKE IT SO THAT NEW ENTRIES GET ADDED TO BELOW TABLES IMMEDIATELY IF THEY EXIST.
"""

def create_index(engine, table_name, column_name):
    """Create an index on a specified column."""
    sql = f"CREATE INDEX idx_{column_name} ON {table_name} ({column_name});"
    execute_sql(engine, sql)

def create_customers_table(engine, staging_table_name):
    """Create a normalized customers table."""
    sql = f"""
        CREATE TABLE customers AS
        SELECT DISTINCT customer_id, age, gender, subscription_status
        FROM {staging_table_name};
    """
    execute_sql(engine, sql)

def create_products_table(engine, staging_table_name):
    """Create a normalized products table."""

    sql = f"""
        CREATE TABLE products AS
        SELECT DISTINCT item_purchased, category, size, color, season
        FROM {staging_table_name};
    """
    execute_sql(engine, sql)

def create_purchases_table(engine, staging_table_name):
    """Create a normalized purchases table."""
    sql = f"""
        CREATE TABLE purchases AS
        SELECT customer_id, item_purchased, purchase_amount, location, review_rating
        FROM {staging_table_name};
    """
    execute_sql(engine, sql)

def update_missing_review_ratings(engine):
    """Update missing review ratings with the average rating."""
    sql = """
        WITH avg_rating AS (
            SELECT AVG(review_rating) AS avg_rating FROM purchases WHERE review_rating IS NOT NULL
        )
        UPDATE purchases
        SET review_rating = avg_rating.avg_rating
        FROM avg_rating
        WHERE review_rating IS NULL;
    """
    execute_sql(engine, sql) 

# ETL process
def etl(data_file_path, engine, table_schema, staging_table_name, primary_keys):
    data = extract(data_file_path)
    staging_database_schema(engine, staging_table_name)
    load_to_staging(data, engine, table_schema, primary_keys)
    transform_and_load(engine, staging_table_name)

def transform_and_load(engine, staging_table_name):
    """Orchestrate the transformation and loading of data."""
    convert_yes_no_to_boolean(engine, staging_table_name)
    # check if engine has been normalized before
    if not inspect(engine).has_table("customers"):
        create_index(engine, staging_table_name, 'customer_id')
        create_customers_table(engine, staging_table_name)
        create_purchases_table(engine, staging_table_name)
        create_products_table(engine, staging_table_name)        

    update_missing_review_ratings(engine)

if __name__ == "__main__":
    # Load configuration
    CONFIG = load_config('config.yaml')
    CSV_FILE_PATH = CONFIG['csv_file_path']
    DB_CONFIG = CONFIG['db_config']
    LOG_FILE_PATH = CONFIG['log_file_path']
    PRIMARY_KEYS = ["customer_id"]
    STAGING_TABLE_NAME = 'staging_customer_data'
    META_DATA = MetaData()
    TABLE_SCHEMA = Table(STAGING_TABLE_NAME, META_DATA,
        Column('customer_id', Integer, primary_key=True),
        Column('category', String),
        Column('age', Integer),
        Column('gender', String),
        Column('location', String),
        Column('color', String),
        Column('size', String),
        Column('season', String),
        Column('item_purchased', String),
        Column('purchase_amount', Float),
        Column('review_rating', Float),
        Column('subscription_status', String),
        Column('payment_method', String),
        Column('shipping_type', String),
        Column('discount_applied', String),
        Column('promo_code_used', String),
        Column('previous_purchase', String),
        Column('preferred_payment_method', String),
        Column('frequency_of_purchase', String),
    )

    # Set up logging
    logging.basicConfig(
        filename='etl_process.log',  # The log file path and name
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'  # Optional: This formats the timestamp
    )

    # Create SQLAlchemy engine
    engine = create_engine(
        f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
    )

    try:
        # Run the ETL pipeline
        etl(CSV_FILE_PATH, engine, TABLE_SCHEMA, 'staging_customer_data', PRIMARY_KEYS)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        engine.dispose()
