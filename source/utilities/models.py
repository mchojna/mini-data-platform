"""SQLAlchemy ORM models for customers, products, orders, and order items."""

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Customer(Base):
    """
    Customer model representing customer information in the business system.
    """

    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    registration_date = Column(Date, nullable=False)

    # Relationship with orders
    orders = relationship("Order", back_populates="customer")

    def __repr__(self):
        return f"<Customer(id={self.customer_id}, name='{self.name}')>"


class Product(Base):
    """
    Product model representing items available for purchase.
    """

    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category = Column(String(50), nullable=False)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False)

    # Relationship with order items
    order_items = relationship("OrderItem", back_populates="product")

    def __repr__(self):
        return (
            f"<Product(id={self.product_id}, name='{self.name}', price='{self.price}')>"
        )


class Order(Base):
    """
    Order model representing customer purchases.
    """

    __tablename__ = "orders"

    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=False)
    order_date = Column(Date, nullable=False)
    total_amount = Column(Float, nullable=False)

    # Relationship with cutomer and item
    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")

    def __repr__(self):
        return f"<Order(id={self.order_id}, customer_id={self.customer_id}, total=${self.total_amount})>"


class OrderItem(Base):
    """
    OrderItem model representing individual items within an order.
    """

    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.order_id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.product_id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)

    # Relationship with order and product
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")

    def __repr__(self):
        return f"<OrderItem(order_id={self.order_id}, product_id={self.product_id}, quantity={self.quantity})>"
