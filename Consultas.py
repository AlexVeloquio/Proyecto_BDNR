from cassandra.cluster import Cluster
from datetime import datetime

def connect():
    cluster = Cluster(["localhost"])
    session = cluster.connect("ecommerce")
    return session

def consultar_carrito(session, usercp):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        SELECT product_id, quantity, added_at
        FROM cart_by_user
        WHERE user_id = ?
        ORDER BY product_id ASC
    """)
    rows = session.execute(query, [user_id])
    for row in rows:
        print(f"- {row.product_id}: {row.quantity} unidades (agregado el {row.added_at})")

def consultar_historial_compras(session, usercp):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        SELECT order_id, product_id, product_name, price, quantity, purchased_at
        FROM purchase_history_by_user
        WHERE user_id = ?
        ORDER BY order_id DESC
    """)
    rows = session.execute(query, [user_id])
    
    print(f"\nHistorial de compras para {user_id}:")
    encontrado = False
    for row in rows:
        encontrado = True
        print(f"- {row.product_name} ({row.product_id}) x{row.quantity} a ${row.price} el {row.purchased_at}")
    if not encontrado:
        print("No se encontraron compras registradas.")

def consultar_tickets_soporte(session, usercp):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        SELECT ticket_id, subject, status, message, response, create_at, responded_at 
        FROM support_ticket_by_user 
        WHERE user_id = ?
        ORDER BY ticket_id DESC
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

def agregar_producto_carrito(session, usercp, product_id, quantity):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        INSERT INTO cart_by_user (user_id, product_id, quantity, added_at)
        VALUES (?, ?, ?, ?)
    """)
    session.execute(query, (user_id, product_id, quantity, datetime.utcnow()))
    print(f"Producto '{product_id}' agregado al carrito de '{user_id}'.")

def eliminar_producto_carrito(session, usercp, product_id):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        DELETE FROM cart_by_user WHERE user_id = ? AND product_id = ?
    """)
    session.execute(query, (user_id, product_id))
    print(f"Producto '{product_id}' eliminado del carrito de '{user_id}'.")

def actualizar_username(session, usercp, nuevo_username):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        UPDATE users SET username = ? WHERE user_id = ?
    """)
    session.execute(query, (nuevo_username, user_id))

def actualizar_telefono(session, usercp, nuevo_telefono):
    user_id = usercp.replace("_", "")
    query = session.prepare("""
        UPDATE users SET phone = ? WHERE user_id = ?
    """)
    session.execute(query, (nuevo_telefono, user_id))

