from cassandra.cluster import Cluster

def connect():
    cluster = Cluster(["localhost"])
    session = cluster.connect("ecommerce")
    return session

def consultar_carrito(session, user_id):
    query = session.prepare("""
        SELECT product_id, quantity, added_at 
        FROM cart_by_user 
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    print(f"\nCarrito de {user_id}:")
    for row in rows:
        print(f"- {row.product_id} | Cantidad: {row.quantity} | Agregado: {row.added_at}")

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

def consultar_tickets_soporte(session, user_id):
    query = session.prepare("""
        SELECT ticket_id, subject, status, message, response, create_at, responded_at 
        FROM support_ticket_by_user 
        WHERE user_id = ?
    """)
    rows = session.execute(query, [user_id])
    print(f"\nTickets de soporte para {user_id}:")
    for row in rows:
        print(f"- Ticket {row.ticket_id} | Estado: {row.status} | Asunto: {row.subject}")
        print(f"  - Mensaje: {row.message}")
        print(f"  - Respuesta: {row.response}")
        print(f"  - Fechas: {row.create_at} -> {row.responded_at}")
    
def menu_querys(session):
    while True:
        print("""
======== CONSULTAS CASSANDRA ========
1. Ver carrito de un usuario
2. Ver historial de compras
3. Ver recomendaciones personalizadas
4. Ver productos en tendencia por fecha
5. Ver tickets de soporte de un usuario
6. Salir
""")
        try:
            opcion = int(input("Seleccione una opción: "))
            if opcion == 1:
                user_id = input("Ingrese el user_id: ")
                consultar_carrito(session, user_id)
            elif opcion == 2:
                user_id = input("Ingrese el user_id: ")
                consultar_historial_compras(session, user_id)
            elif opcion == 3:
                user_id = input("Ingrese el user_id: ")
                consultar_recomendaciones(session, user_id)
            elif opcion == 4:
                fecha = input("Ingrese la fecha (YYYY-MM-DD): ")
                consultar_tendencias_por_fecha(session, fecha)
            elif opcion == 5:
                user_id = input("Ingrese el user_id: ")
                consultar_tickets_soporte(session, user_id)
            elif opcion == 6:
                print("Saliendo del menú de consultas.")
                break
            else:
                print("Opción inválida.")
        except ValueError:
            print("Error: Ingrese un número válido.")