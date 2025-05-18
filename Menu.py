import time
import Poblation
import ModelCassandra
import Consultas
import ModelDgraph
import os
import pydgraph
from pymongo import MongoClient, TEXT
from db import create_indexes
import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

# Conexión directa con el servidor Dgraph usando gRPC.
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

# Crea el cliente principal de Dgraph: realizar operaciones de alto nivel como queries, mutaciones y transacciones.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

# Crear colecciones con índices
def create_indexes():
    db.products.create_index([("category", 1)])
    db.products.create_index([("price", 1), ("category", 1)])
    db.products.create_index([("title", TEXT), ("description", TEXT)])
    db.reviews.create_index([("product_id", 1)])
    db.trending.create_index([("last_week_score", -1)])

def print_menu():
    print("""\n########## Ecommerce ##########
1.- Perfil 
2.- Carrito de compras
3.- Favoritos
4.- Productos
5.- Salir""")

def print_menuPerfil():
    print("""\n########## Ecommerce ##########
1.- Ver informacion 
2.- Actualizar username
3.- Actualizar telefono 
4.- Ver devoluciones
5.- Generar devolucion 
6.- Soporte
7.- Ver historial de compras
8.- Salir""")

def print_menuCarro():
    print("""\n########## Ecommerce ##########
1.- Ver informacion de carro
2.- Agregar productos al carro
3.- Eliminar productos del carro
4.- Salir""")   
     
def print_menuProductos():
    print("""\n########## Ecommerce ##########
1.- Mostrar todos los productos
2.- Buscar producto 
3.- Buscar producto por categoria y precio
4.- Buscar producto por palabra clave
5.- Ver reseñas de un producto
6.- Agregar reseña a un producto
7.- Ver productos populares 
8.- Mostrar productos y reseñas por categoria
9.- Mostrar los productos mejores calificados
10.- Salir""")
        
def print_menuFavoritos():
    print("""\n########## Ecommerce ##########
1.- Ver Favoritos 
2.- Guardar Favoritos
3.- Eliminar de favoritos
4.- Recomendacion por favoritos
5.- Salir""")   
    
def main():
    user = input("Ingresa tu usuario(Ej. user_1):")
    usercp = user
    while True:
        print_menu()
        option = int(input("Ingresa una opcion:"))
        
        if option == 1:  # PERFIL
            while True:
                print_menuPerfil()
                opcion = int(input("Ingresa una opcion:"))
                if opcion == 1:
                    print("Mostrar información del usuario")
                    ModelDgraph.search_users(client, user)
                elif opcion == 2:
                    print(" Actualizar username")
                    username = input("Ingresa un nombre de usuario:")
                    ModelDgraph.actualizar_username(client, user, username)
                    Consultas.actualizar_username(session,user, username)
                    user=username
                elif opcion == 3:
                    print(" Actualizar teléfono")
                    telefono = input("Ingrese un numero de telefono:")
                    ModelDgraph.actualizar_telefono(client, user, telefono)
                    Consultas.actualizar_telefono(session, user, telefono)
                elif opcion == 4:
                    print(" Ver devoluciones")
                    ModelDgraph.devoluciones_por_usuario(client, user)
                elif opcion == 5:
                    print("Generar devolución")
                    producto = input("Producto a devolver: ")
                    motivo = input("Motivo: ")
                    ModelDgraph.registrar_devolucion(client, producto, motivo, user)
                elif opcion == 6:
                    print("Ver tickets de soporte")
                    Consultas.consultar_tickets_soporte(session, user)
                elif opcion == 7:
                    print("Ver historial de compra")
                    Consultas.consultar_historial_compras(session, user)
                elif opcion == 8:
                    break
                else:
                    print("Opción inválida")

        elif option == 2:  # CARRITO
            while True:
                print_menuCarro()
                opcion = int(input("Ingresa una opcion:"))
                if opcion == 1:
                    print("Ver carrito")
                    user_id = usercp
                    Consultas.consultar_carrito(session, user_id)
                elif opcion == 2:
                    print("Agregar producto al carrito")
                    product_id = input("ID del producto a agregar: ")
                    cantidad = int(input("Cantidad: "))
                    Consultas.agregar_producto_carrito(session, user, product_id, cantidad)
                elif opcion == 3:
                    print("Eliminar producto del carrito")
                    product_id = input("ID del producto a eliminar: ")
                    Consultas.eliminar_producto_carrito(session, user, product_id)
                elif opcion == 4:
                    break
                else:
                    print("Opción inválida")

        elif option == 3:  # FAVORITOS
            while True:
                print_menuFavoritos()
                opcion = int(input("Ingresa una opcion:"))
                if opcion == 1:
                    print("Ver favoritos")
                    ModelDgraph.favoritos_del_usuario(client, user)
                elif opcion == 2:
                    print("Guardar en favoritos")
                    producto = input("Producto: ")
                    ModelDgraph.guardar_en_favoritos(client, user, producto)
                elif opcion == 3:
                    print("Eliminar de favoritos")
                    producto = input("Producto:")
                    ModelDgraph.quitar_de_favoritos(client, user, producto)
                elif opcion == 4:
                    print("Recomendaciones por favoritos")
                    ModelDgraph.recomendaciones_por_categoria(client, user)
                elif opcion == 5:
                    break
                else:
                    print("Opción inválida")

        elif option == 4:  # PRODUCTOS
            while True:
                print_menuProductos()
                opcion = int(input("Ingresa una opcion:"))
                if opcion == 1:
                    print("Mostrar todos los productos")
                    model.print_all_products()
                elif opcion == 2:
                    print("Buscar producto")
                    titulo = input("Ingresa el nombre del producto:")
                    result= model.get_product_by_title(titulo)
                    print(f"=== Detalles del Producto: {title} ===\n")
                    print(f"Descripción: {result['description']}")
                    print(f"Precio: ${result['price']}")
                    print(f"Stock: {result['stock']}")
                    print(f"Categoría: {result['category']}\n")
                elif opcion == 3:
                    print("Buscar por categoría y precio")
                    print("Formal Wear, Bottoms, Tops, Outerwear, " \
                    "\nSportswear, Anime Collection, Accessories, " \
                    "\nDresses, Footwear, K-Pop Style, Oversized, " \
                    "\nCasual Wear, Underwear")
                    category = input("Ingresa la categoria:")
                    min_price = int(input("Precio mínimo: "))
                    max_price = int(input("Precio máximo: "))
                    results = model.get_products_by_category_price(category, min_price, max_price)
                    print(f"=== Productos en '{category}' entre ${min_price} y ${max_price} ===\n")
                    for p in results:
                        print(f"• {p['title']} - ${p['price']} ({p['category']})")
                elif opcion == 4:
                    print("Buscar por palabra clave")
                    keyword = input("Palabra clave: ")
                    min_price = int(input("Precio mínimo: "))
                    max_price = int(input("Precio máximo: "))
                    results = model.search_products_by_keyword(keyword, min_price, max_price)
                    print(f"=== Resultados de búsqueda: '{keyword}' entre ${min_price} y ${max_price} ===\n")
                    for p in results:
                     print(f"• {p['title']} - ${p['price']} ({p['category']})")
                elif opcion == 5:
                    print("Ver reseñas del producto")
                    title = input("Título del producto: ")
                    reviews = model.get_reviews_by_product_title(title)
                    if reviews:
                        for r in reviews:
                         print(f"Usuario: {r['username']} - Calificación: {r['rating']} - Comentario: {r['comment']}")
                    else:
                         print("No se encontraron reseñas para ese producto.")
                elif opcion == 6:
                    print("Agregar reseña al producto")
                    title = input("Título del producto: ")
                    username = user
                    rating = int(input("Calificación (1-5): "))
                    comment = input("Comentario: ")
                    if model.add_review_by_title_and_username(title, username, rating, comment):
                        print("Reseña agregada correctamente.")
                    else:
                        print("Producto no encontrado.")
                elif opcion == 7:
                    print("Ver productos populares")
                    results = model.get_popular_products()
                    print("=== Productos Populares de la Última Semana ===\n")
                    for t in results:
                        print(f"Título: {t['product_title']} - Score: {t['last_week_score']} - Views: {t['views']} - Sales: {t['sales']}")
                elif opcion == 8:
                    print("Mostrar productos y reseñas por categoría")
                    category = input("Categoría: ")
                    results = model.get_products_with_reviews(category)
                    for p in results:
                        print(p)
                elif opcion == 9:
                    print("Productos mejores calificados")
                    min_rating = float(input("Rating mínimo: "))
                    results = model.get_top_rated_products(min_rating)
                    print(f"=== Productos mejor calificados con rating > {min_rating} ===\n")
                    for p in results:
                        print(f"Título: {p['title']} - Precio: ${p['price']} - Categoría: ({p['category']}) - Rating: {p['rating']:.2f}")
                elif opcion == 10:
                    break
                else:
                    print("Opción inválida")

        elif option == 5:
            print("Gracias por usar el sistema.")
            ModelCassandra.drop_all_tables(session)
            ModelDgraph.drop_all(client)
            break

        else:
            print("Opción inválida")


if __name__ == '__main__':
    session = ModelCassandra.connect()
    client_stub = ModelDgraph.pydgraph.DgraphClientStub('localhost:9080')
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ecommerce"]
    ModelCassandra.create_keyspace_and_table(session)
    Poblation.poblar_tablas()
    # from loader import load_products_csv, load_reviews_csv, load_trending_csv
    # load_products_csv("data/products.csv")
    # load_reviews_csv("data/reviews.csv")
    # load_trending_csv("data/trending.csv")
    print("Datos cargados desde CSV correctamente.")
    create_indexes()
    
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)
    # Crear schema
    ModelDgraph.set_schema(client)
    ModelDgraph.create_data(client_stub)
    print("Datos creados correctamente")
    client = ModelDgraph.pydgraph.DgraphClient(client_stub)
    main()