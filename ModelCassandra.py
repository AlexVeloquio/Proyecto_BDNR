from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

CREATE_KEYSPACE_ECOMMERCE = """
    CREATE KEYSPACE IF NOT EXISTS ecommerce
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
"""
# Tabla de usuarios
CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        birthdate DATE,
        created_at TIMESTAMP,
        PRIMARY KEY (user_id)
    )
"""
# Tabla de carrito
CREATE_CART_TABLE = """
    CREATE TABLE IF NOT EXISTS cart_by_user (
        user_id TEXT,
        product_id TEXT,
        quantity INT,
        added_at TIMESTAMP,
        PRIMARY KEY ((user_id), product_id)
    )
"""
# Tabla de historial de compras
CREATE_PURCHASE_HISTORY_TABLE = """
    CREATE TABLE IF NOT EXISTS purchase_history_by_user (
        user_id TEXT,
        order_id TIMEUUID,
        product_id TEXT,
        product_name TEXT,
        price DECIMAL,
        quantity INT,
        purchased_at TIMESTAMP,
        PRIMARY KEY ((user_id), order_id)
    ) WITH CLUSTERING ORDER BY (order_id DESC)
"""
# Tabla de recomendaciones personalizadas
CREATE_RECOMMENDATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS recommendations_by_user (
        user_id TEXT,
        recommended_product_id TEXT,
        score FLOAT,
        reason TEXT,
        PRIMARY KEY ((user_id), recommended_product_id)
    )
"""
# Productos en tendencia
CREATE_TRENDING_PRODUCTS_TABLE = """
    CREATE TABLE IF NOT EXISTS trending_products (
        date DATE,
        product_id TEXT,
        name TEXT,
        total_sales INT,
        PRIMARY KEY ((date), total_sales, product_id)
    ) WITH CLUSTERING ORDER BY (total_sales DESC)
"""
CREATE_SUPPORT_TICKETS_TABLE = """
    CREATE TABLE IF NOT EXISTS support_ticket_by_user (
        user_id TEXT,
        ticket_id TIMEUUID,
        subject TEXT,
        status TEXT,
        message TEXT,
        response TEXT,
        create_at TIMESTAMP,
        responded_at TIMESTAMP,
        PRIMARY KEY ((user_id), ticket_id)
    ) WITH CLUSTERING ORDER BY (ticket_id DESC)
"""
def drop_all_tables(session):
    session.set_keyspace("ecommerce")
    tables = [
        "users",
        "cart_by_user",
        "purchase_history_by_user",
        "recommendations_by_user",
        "trending_products",
        "support_ticket_by_user"
    ]

    for table in tables:
        try:
            session.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception as e:
            print(f"Error al eliminar la tabla '{table}': {e}")

def connect():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    return session

def create_keyspace_and_table(session):
    session.execute(CREATE_KEYSPACE_ECOMMERCE)
    session.set_keyspace("ecommerce")
    session.execute (CREATE_USERS_TABLE)
    session.execute(CREATE_CART_TABLE)
    session.execute (CREATE_PURCHASE_HISTORY_TABLE)
    session.execute (CREATE_RECOMMENDATIONS_TABLE)
    session.execute (CREATE_TRENDING_PRODUCTS_TABLE)
    session.execute(CREATE_SUPPORT_TICKETS_TABLE)