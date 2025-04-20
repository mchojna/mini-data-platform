import os
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from models import Base, Customer, Product, Order, OrderItem

# Load environment
load_dotenv(".env")

# Database connection parameters
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

# Database connection URL
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Paths to CSV files
DATA_DIR = os.getenv("DATA_DIR", "/app/data/")
CUSTOMER_CSV = os.path.join(DATA_DIR, "customers.csv")
PRODUCTS_CSV = os.path.join(DATA_DIR, "products.csv")
ORDERS_CSV = os.path.join(DATA_DIR, "orders.csv")

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Set up handler
console_handler = logging.StreamHandler()

# Set up logging format
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add handler to logger
if not logger.handlers:
    logger.addHandler(console_handler)

class DataInitializer():
    """
    Class to initialize database, load CSV data, and insert records into PostgreSQL.
    """
    def __init__(self, db_url: str, data_dir):
        self.db_url = db_url
        self.data_dir = data_dir
        self.customers_csv = os.path.join(self.data_dir, "customers.csv")
        self.products_csv = os.path.join(self.data_dir, "products.csv")
        self.orders_csv = os.path.join(self.data_dir, "orders.csv")

    def wait_for_postgres(self, engine: Engine, max_retries: int, delay: int) -> bool:
        """Wait for PostgreSQL to be available"""
        for i in range(max_retries):
            try:
                with engine.connect():
                    logger.info("Successfully connected to PostgreSQL")
                    return True
            except Exception:
                logger.info(f"Waiting for PostgreSQL to be available... ({i+1}/{max_retries})")
                time.sleep(delay)
        return False

    def setup_database(self, engine: Engine) -> None:
        """Create database tables"""
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")

    def load_csv_data(self, file_path: str) -> pd.DataFrame:
        """Load data from a CSV file"""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            logger.info(f"Error loading CSV file {file_path}: {e}")
            return pd.DataFrame()

    def insert_customers(self, session: Session, customers_df: pd.DataFrame) -> None:
        """Insert customer data into the database"""
        inserted_customers = 0
        for _, row in customers_df.iterrows():
            # Check if customer already exists
            existing_customer = session.query(Customer).filter_by(customer_id=row['customer_id']).first()
            if not existing_customer:
                customer = Customer(
                    customer_id=row['customer_id'],
                    name=row['name'],
                    email=row['email'],
                    registration_date=datetime.strptime(row['registration_date'], '%Y-%m-%d').date()
                )
                session.add(customer)
                inserted_customers += 1

        session.commit()
        logger.info(f"Inserted {inserted_customers} customers")

    def insert_products(self, session: Session, products_df: pd.DataFrame) -> None:
        """Insert product data into the database"""
        inserted_products = 0
        for _, row in products_df.iterrows():
            # Check if product already exists
            existing_product = session.query(Product).filter_by(product_id=row['product_id']).first()
            if not existing_product:
                product = Product(
                    product_id=row['product_id'],
                    name=row['name'],
                    category=row['category'],
                    price=row['price'],
                    stock=row['stock']
                )
                session.add(product)
                inserted_products += 1

        session.commit()
        logger.info(f"Inserted {inserted_products} products")

    def insert_orders(self, session: Session, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> None:
        """Insert order data into the database"""
        inserted_orders = 0
        for _, row in orders_df.iterrows():
        # Check if order already exists
            existing_order = session.query(Order).filter_by(order_id=row['order_id']).first()
            if not existing_order:
                order = Order(
                    order_id=row['order_id'],
                    customer_id=row['customer_id'],
                    order_date=datetime.strptime(row['order_date'], '%Y-%m-%d').date(),
                    total_amount=row['total_amount']
                )
                session.add(order)
                inserted_orders += 1

                # Generate random order items for each order
                num_items = random.randint(1, 3)
                products = products_df.sample(num_items)

                for _, product in products.iterrows():
                    quantity = random.randint(1, 3)
                    order_item = OrderItem(
                        order_id=row['order_id'],
                        product_id=product['product_id'],
                        quantity=quantity,
                        price=product['price']
                    )
                    session.add(order_item)

        session.commit()
        print(f"Inserted {inserted_orders} orders with items")

    def __call__(self) -> None:
        """Main function to initalize"""
        engine = create_engine(self.db_url)

        # Wait for PostgreSQL to be available
        if not self.wait_for_postgres(engine, max_retries=20, delay=2):
            return

        # Set up database
        self.setup_database(engine=engine)

        # Create session
        with Session(engine) as session:
            try:
                # Load data from CSV file
                customers_df = self.load_csv_data(self.customers_csv)
                products_df = self.load_csv_data(self.products_csv)
                orders_df = self.load_csv_data(self.orders_csv)

                if customers_df.empty or products_df.empty or orders_df.empty:
                    logger.info("One or more CSV files could not be loaded. Exiting.")
                    return

                # Insert data into the database
                self.insert_customers(session, customers_df)
                self.insert_products(session, products_df)
                self.insert_orders(session, orders_df, products_df)

                logger.info("Initialization completed successfully")

            except SQLAlchemyError as e:
                session.rollback()
                logger.info(f"Database error: {e}")
            except Exception as e:
                session.rollback()
                logger.info(f"Error during initialization: {e}")

if __name__ == "__main__":
    data_initializer = DataInitializer(db_url=DB_URL, data_dir=DATA_DIR)
    data_initializer()
