import csv
import datetime
from cassandra.cluster import Cluster
import uuid

def connect():
    cluster = Cluster(["localhost"])
    session = cluster.connect("ecommerce")
    return session

def insert_usuarios(session):
    with open("data/users.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO users (user_id, username, email, phone, birthdate, created_at) VALUES (?, ?, ?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                row['user_id'],
                row['username'],
                row['email'],
                row['phone'],
                datetime.datetime.strptime(row['birthdate'], '%Y-%m-%d').date(),
                datetime.datetime.fromisoformat(row['created_at'])
            ))

def insert_carrito(session):
    with open("data/cart_by_user.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO cart_by_user (user_id, product_id, quantity, added_at) VALUES (?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                row['user_id'],
                row['product_id'],
                int(row['quantity']),
                datetime.datetime.fromisoformat(row['added_at'])
            ))

def insert_compras(session):
    with open("data/purchase_history_by_user.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO purchase_history_by_user (user_id, order_id, product_id, product_name, price, quantity, purchased_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                row['user_id'],
                uuid.UUID(row['order_id']),
                row['product_id'],
                row['product_name'],
                float(row['price']),
                int(row['quantity']),
                datetime.datetime.fromisoformat(row['purchased_at'])
            ))

def insert_recomendaciones(session):
    with open("data/recommendations_by_user.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO recommendations_by_user (user_id, recommended_product_id, score, reason) VALUES (?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                row['user_id'],
                row['recommended_product_id'],
                float(row['score']),
                row['reason']
            ))

def insert_tendencias(session):
    with open("data/trending_products.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO trending_products (date, product_id, name, total_sales) VALUES (?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                datetime.datetime.strptime(row['date'], '%Y-%m-%d').date(),
                row['product_id'],
                row['name'],
                int(row['total_sales'])
            ))

def insert_tickets(session):
    with open("data/support_ticket_by_user.csv", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        query = session.prepare("INSERT INTO support_ticket_by_user (user_id, ticket_id, subject, status, message, response, create_at, responded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        for row in reader:
            session.execute(query, (
                row['user_id'],
                uuid.UUID(row['ticket_id']),
                row['subject'],
                row['status'],
                row['message'],
                row['response'],
                datetime.datetime.fromisoformat(row['create_at']),
                datetime.datetime.fromisoformat(row['responded_at'])
            ))
            
def poblar_tablas():
    session = connect()
    insert_usuarios(session)
    insert_carrito(session)
    insert_compras(session)
    insert_recomendaciones(session)
    insert_tendencias(session)
    insert_tickets(session) 
    print("Datos insertados correctamente en Cassandra.")

if __name__ == "__main__":
    poblar_tablas()
