#!/usr/bin/env python3
"""
Graph ETL Pipeline - Transform relational data into graph structures
"""

import pandas as pd
import numpy as np
from neo4j import GraphDatabase
import logging
from typing import Dict, List, Any
import json
from datetime import datetime

class GraphETLPipeline:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """Initialize Graph ETL Pipeline"""
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.logger = logging.getLogger(__name__)
        
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            self.logger.info("Database cleared")
    
    def create_constraints(self):
        """Create unique constraints for better performance"""
        constraints = [
            "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT employee_id IF NOT EXISTS FOR (e:Employee) REQUIRE e.id IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    self.logger.warning(f"Constraint may already exist: {e}")
    
    def load_customers(self, customers_df: pd.DataFrame):
        """Load customer nodes"""
        query = """
        UNWIND $customers AS customer
        CREATE (c:Customer {
            id: customer.id,
            name: customer.name,
            email: customer.email,
            city: customer.city,
            country: customer.country,
            segment: customer.segment,
            registration_date: date(customer.registration_date),
            lifetime_value: customer.lifetime_value
        })
        """
        
        customers_data = customers_df.to_dict('records')
        with self.driver.session() as session:
            session.run(query, customers=customers_data)
            self.logger.info(f"Loaded {len(customers_data)} customers")
    
    def load_products(self, products_df: pd.DataFrame):
        """Load product nodes with categories"""
        # Create products
        query = """
        UNWIND $products AS product
        CREATE (p:Product {
            id: product.id,
            name: product.name,
            category: product.category,
            price: product.price,
            cost: product.cost,
            margin: product.margin,
            launch_date: date(product.launch_date)
        })
        """
        
        products_data = products_df.to_dict('records')
        with self.driver.session() as session:
            session.run(query, products=products_data)
            self.logger.info(f"Loaded {len(products_data)} products")
        
        # Create category relationships
        category_query = """
        MATCH (p:Product)
        MERGE (c:Category {name: p.category})
        MERGE (p)-[:BELONGS_TO]->(c)
        """
        
        with self.driver.session() as session:
            session.run(category_query)
            self.logger.info("Created product-category relationships")
    
    def load_orders(self, orders_df: pd.DataFrame):
        """Load order nodes and relationships"""
        query = """
        UNWIND $orders AS order
        MATCH (c:Customer {id: order.customer_id})
        MATCH (p:Product {id: order.product_id})
        CREATE (o:Order {
            id: order.id,
            order_date: date(order.order_date),
            quantity: order.quantity,
            unit_price: order.unit_price,
            total_amount: order.total_amount,
            discount: order.discount
        })
        CREATE (c)-[:PLACED]->(o)
        CREATE (o)-[:CONTAINS]->(p)
        """
        
        orders_data = orders_df.to_dict('records')
        with self.driver.session() as session:
            session.run(query, orders=orders_data)
            self.logger.info(f"Loaded {len(orders_data)} orders with relationships")
    
    def create_customer_similarity_relationships(self):
        """Create SIMILAR_TO relationships between customers based on purchase behavior"""
        query = """
        MATCH (c1:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
        MATCH (c2:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p)
        WHERE c1.id < c2.id
        WITH c1, c2, count(p) as common_products
        WHERE common_products >= 2
        CREATE (c1)-[:SIMILAR_TO {strength: common_products}]->(c2)
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            self.logger.info("Created customer similarity relationships")
    
    def create_product_co_purchase_relationships(self):
        """Create CO_PURCHASED relationships between products"""
        query = """
        MATCH (p1:Product)<-[:CONTAINS]-(:Order)<-[:PLACED]-(c:Customer)
        MATCH (c)-[:PLACED]->(:Order)-[:CONTAINS]->(p2:Product)
        WHERE p1.id < p2.id
        WITH p1, p2, count(c) as co_purchases
        WHERE co_purchases >= 2
        CREATE (p1)-[:CO_PURCHASED {frequency: co_purchases}]->(p2)
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            self.logger.info("Created product co-purchase relationships")
    
    def calculate_customer_metrics(self):
        """Calculate and store customer metrics"""
        query = """
        MATCH (c:Customer)-[:PLACED]->(o:Order)
        WITH c, 
             count(o) as total_orders,
             sum(o.total_amount) as total_spent,
             avg(o.total_amount) as avg_order_value,
             max(o.order_date) as last_order_date
        SET c.total_orders = total_orders,
            c.total_spent = total_spent,
            c.avg_order_value = avg_order_value,
            c.last_order_date = last_order_date,
            c.customer_tier = CASE 
                WHEN total_spent > 5000 THEN 'VIP'
                WHEN total_spent > 2000 THEN 'Premium'
                ELSE 'Standard'
            END
        """
        
        with self.driver.session() as session:
            session.run(query)
            self.logger.info("Updated customer metrics")
    
    def run_graph_analytics(self):
        """Run various graph analytics algorithms"""
        analytics_queries = {
            "pagerank": """
                CALL gds.pageRank.write('customer-similarity', {
                    writeProperty: 'pagerank'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
            """,
            "community_detection": """
                CALL gds.louvain.write('customer-similarity', {
                    writeProperty: 'community'
                })
                YIELD communityCount, nodePropertiesWritten
                RETURN communityCount, nodePropertiesWritten
            """,
            "centrality": """
                CALL gds.betweenness.write('customer-similarity', {
                    writeProperty: 'betweenness'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
            """
        }
        
        # First create graph projection
        projection_query = """
        CALL gds.graph.project(
            'customer-similarity',
            'Customer',
            'SIMILAR_TO',
            {
                relationshipProperties: 'strength'
            }
        )
        """
        
        with self.driver.session() as session:
            try:
                session.run(projection_query)
                self.logger.info("Created graph projection")
                
                for name, query in analytics_queries.items():
                    try:
                        result = session.run(query)
                        self.logger.info(f"Completed {name} analysis")
                    except Exception as e:
                        self.logger.warning(f"Analytics {name} failed: {e}")
                        
            except Exception as e:
                self.logger.warning(f"Graph projection failed: {e}")

def main():
    """Main ETL execution"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize pipeline
    pipeline = GraphETLPipeline("bolt://localhost:7687", "neo4j", "password")
    
    try:
        # Clear and setup
        pipeline.clear_database()
        pipeline.create_constraints()
        
        # Load sample data (you would replace with actual data loading)
        customers_df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Customer {i}' for i in range(1, 101)],
            'email': [f'customer{i}@example.com' for i in range(1, 101)],
            'city': np.random.choice(['New York', 'London', 'Tokyo', 'Sydney'], 100),
            'country': np.random.choice(['USA', 'UK', 'Japan', 'Australia'], 100),
            'segment': np.random.choice(['Enterprise', 'SMB', 'Consumer'], 100),
            'registration_date': pd.date_range('2020-01-01', periods=100, freq='D'),
            'lifetime_value': np.random.uniform(100, 10000, 100)
        })
        
        products_df = pd.DataFrame({
            'id': range(1, 21),
            'name': [f'Product {i}' for i in range(1, 21)],
            'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 20),
            'price': np.random.uniform(10, 1000, 20),
            'cost': np.random.uniform(5, 500, 20),
            'margin': np.random.uniform(0.1, 0.5, 20),
            'launch_date': pd.date_range('2019-01-01', periods=20, freq='M')
        })
        
        # Generate orders
        orders_data = []
        for i in range(1, 201):
            orders_data.append({
                'id': i,
                'customer_id': np.random.randint(1, 101),
                'product_id': np.random.randint(1, 21),
                'order_date': pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 365)),
                'quantity': np.random.randint(1, 5),
                'unit_price': np.random.uniform(10, 1000),
                'total_amount': np.random.uniform(10, 5000),
                'discount': np.random.uniform(0, 0.3)
            })
        
        orders_df = pd.DataFrame(orders_data)
        
        # Load data
        pipeline.load_customers(customers_df)
        pipeline.load_products(products_df)
        pipeline.load_orders(orders_df)
        
        # Create relationships
        pipeline.create_customer_similarity_relationships()
        pipeline.create_product_co_purchase_relationships()
        
        # Calculate metrics
        pipeline.calculate_customer_metrics()
        
        # Run analytics
        pipeline.run_graph_analytics()
        
        print("Graph ETL pipeline completed successfully!")
        
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()