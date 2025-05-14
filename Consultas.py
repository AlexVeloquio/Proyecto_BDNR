from cassandra.cluster import Cluster
from datetime import datetime
def formatear_user_id(username):
    return username.replace("_", "")

def connect():
    cluster = Cluster(["localhost"])
    session = cluster.connect("ecommerce")
    return session


def consultar_carrito(session, user):
    user_id = user.replace("_", "")
    query = session.prepare("""
        SELECT product_id, quantity, added_at
        FROM cart_by_user
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    for row in rows:
        print(f"- {row.product_id}: {row.quantity} unidades (agregado el {row.added_at})")


def consultar_historial_compras(session, user_id):
    query = session.prepare("""
        SELECT product_id, product_name, price, quantity, purchased_at 
        FROM purchase_history_by_user 
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    print(f"\nCompras de {user_id}:")
    for row in rows:
        print(f"- {row.product_name} ({row.product_id}) x{row.quantity} a ${row.price} el {row.purchased_at}")

def consultar_recomendaciones(session, user_id):
    query = session.prepare("""
        SELECT recommended_product_id, score, reason 
        FROM recommendations_by_user 
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    print(f"\nRecomendaciones para {user_id}:")
    for row in rows:
        print(f"- {row.recommended_product_id} | Score: {row.score} | Razón: {row.reason}")

def consultar_tendencias_por_fecha(session, date_str):
    query = session.prepare("""
        SELECT product_id, name, total_sales 
        FROM trending_products 
        WHERE date = ?
    """)
    rows = session.execute(query, [date_str])
    print(f"\nProductos en tendencia ({date_str}):")
    for row in rows:
        print(f"- {row.name} ({row.product_id}) | Ventas: {row.total_sales}")

def consultar_tickets_soporte(session, user):
    user_id = user.replace("_", "")
    query = session.prepare("""
        SELECT ticket_id, subject, status, message, response, create_at, responded_at 
        FROM support_ticket_by_user 
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    
    print(f"\nTickets de soporte para {user_id}:")

    encontrados = False
    for row in rows:
        encontrados = True
        print(f"- Ticket {row.ticket_id} | Estado: {row.status} | Asunto: {row.subject}")
        print(f" Mensaje: {row.message}")
        print(f" Respuesta: {row.response}")
        print(f" Fechas: {row.create_at} → {row.responded_at}")
    
    if not encontrados:
        print("No hay tickets registrados para este usuario.")

def agregar_producto_carrito(session, user, product_id, quantity):
    user_id = user.replace("_", "")
    query = session.prepare("""
        INSERT INTO cart_by_user (user_id, product_id, quantity, added_at)
        VALUES (?, ?, ?, ?)
    """)
    session.execute(query, (user_id, product_id, quantity, datetime.utcnow()))
    print(f"Producto '{product_id}' agregado al carrito de '{user_id}'.")

def eliminar_producto_carrito(session, user, product_id):
    user_id = user.replace("_", "")
    query = session.prepare("""
        DELETE FROM cart_by_user WHERE user_id = ? AND product_id = ?
    """)
    session.execute(query, (user_id, product_id))
    print(f"Producto '{product_id}' eliminado del carrito de '{user_id}'.")

def actualizar_username(session, user, nuevo_username):
    user_id = user.replace("_", "")
    query = session.prepare("""
        UPDATE users SET username = ? WHERE user_id = ?
    """)
    session.execute(query, (nuevo_username, user_id))
    print(f"Username de '{user_id}' actualizado a: {nuevo_username}")

def actualizar_telefono(session, user, nuevo_telefono):
    user_id = user.replace("_", "")
    query = session.prepare("""
        UPDATE users SET phone = ? WHERE user_id = ?
    """)
    session.execute(query, (nuevo_telefono, user_id))
    print(f"Teléfono de '{user_id}' actualizado a: {nuevo_telefono}")