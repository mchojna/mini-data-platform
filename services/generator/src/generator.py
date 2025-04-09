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
from faker import Faker

# Load environment
load_dotenv(".env")

# Database connection parameters
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# Database connection URL
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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

class Generator():
    def __init__(self, db_url: str):
        self.db_url = db_url
        
        self.faker = Faker()
    
    def wait_for_postgres(self, engine: Engine, max_retries: int, delay: int) -> bool:
        """Wait for PostgreSQL to be available"""
        for i in range(max_retries):
            try:
                with engine.connect() as conn:
                    logger.info("Successfully connected to PostgreSQL")
                    return True
            except Exception as e:
                logger.info(f"Waiting for PostgreSQL to be available... ({i+1}/{max_retries})")
                time.sleep(delay)
        return False
    
    def setup_database(self, engine: Engine) -> None:
        """Create database tables"""
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    
    def generate_customer(self, session):
        """Generate a new customer"""
        # Get the highest customer id
        result = session.query(Customer).order_by(Customer.customer_id.desc()).first()
        next_id = result.customer_id + 1 if result else 1
        
        # Generate a new customer
        customer = Customer(
            customer_id=next_id,
            name=self.faker.name(),
            email=self.faker.unique.email(),
            registration_date=datetime.now().date(),
        )
        
        session.add(customer)
        session.commit()
        logger.info(f"Generated new customer: {customer}")
    
    def generate_order(self, session):
        "Generate a new order"
        customers = session.query(Customer).all()
        if not customers:
            logger.info("No customers found")
            return
        
        customer = random.choice(customers)
        
        result = session.query(Order).order_by(Order.order_id.desc()).first()
        next_id = result.order_id + 1 if result else 1
        
        products = session.query(Product).all()
        if not products:
            logger.info("No products found")
            return

        selected_products = random.sample(products, min(random.randint(1, 3), len(products)))
        
        total_amount = sum(product.price * random.randint(1, 3) for product in selected_products)
        
        order = Order(
            order_id=next_id,
            customer_id=customer.customer_id,
            order_date=datetime.now().date(),
            total_amount=total_amount
        )
        
        session.add(order)
        session.flush()
        
        for product in selected_products:
            quantity = random.randint(1, 3)
            order_item = OrderItem(
                order_id=order.order_id,
                product_id=product.product_id,
                quantity=quantity,
                price=product.price
            )
            session.add(order_item)
            
        session.commit()
        print(f"Generated new order: {order} with {len(selected_products)} items")

    def generate_product(self, session):
        "Generate a new product"
        # Get a random product
        products = session.query(Product).all()
        if not products:
            logger.info("No products found")
            return

        product = random.choince(products)
        
        # Update stock
        old_stock = product.stock
        product.stock = max(0, product.stock + random.randint(-10, 20))
    
        session.commit()
        logger.info(f"Update product {product.name} stock: {old_stock} -> {product.stock}")
        
    
    def __call__(self) -> None:
        """Main function to generate"""
        engine = create_engine(self.db_url)
        
        # Wait for PostgreSQL to be available
        if not self.wait_for_postgres(engine, max_retries=20, delay=2):
            return
        
        # Set up database
        self.setup_database(engine=engine)
        
        # Create session
        while True:
            with Session(engine) as session:
                try:
                    action = random.choice(["customer", "order", "product", "order", "product"])
                    
                    if action == "customer":
                        self.generate_customer(session)
                    elif action == "order":
                        self.generate_order(session)
                    elif action == "product":
                        self.generate_product(session)
                    
                except SQLAlchemyError as e:
                    session.rollback()
                    logger.info(f"Database error: {e}")
                except Exception as e:
                    session.rollback()
                    logger.info(f"Error during initialization: {e}")
            
            wait_time = random.randint(2, 10)
            logger.info(f"Waiting {wait_time} seconds before next action...")
            time.sleep(wait_time)
    
if __name__ == "__main__":
    time.sleep(60)
    generator = Generator(db_url=DB_URL)
    generator()