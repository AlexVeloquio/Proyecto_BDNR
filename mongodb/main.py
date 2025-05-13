from pymongo import MongoClient, TEXT
from db import create_indexes
import model

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

# Crear colecciones con índices
def create_indexes():
    db.products.create_index([("category", 1)])
    db.products.create_index([("price", 1), ("category", 1)])
    db.products.create_index([("title", TEXT), ("description", TEXT)])
    db.reviews.create_index([("product_id", 1)])
    db.trending.create_index([("last_week_score", -1)])

def main_menu():
    while True:
        print("\n--- MENÚ E-COMMERCE ---")
        print("1. Agregar producto")
        print("2. Buscar productos por categoría y precio")
        print("3. Buscar productos por palabra clave")
        print("4. Ver reseñas de un producto")
        print("5. Agregar reseña")
        print("6. Ver productos mejor calificados")
        print("7. Ver productos populares")
        print("8. Mostrar productos y reseñas por categoría")
        print("9. Cargar datos desde CSV")
        print("0. Salir")
        
        choice = input("Selecciona una opción: ")
        
        if choice == "1":
            title = input("Título: ")
            desc = input("Descripción: ")
            price = int(input("Precio: "))
            stock = int(input("Stock: "))
            category = input("Categoría: ")
            model.add_product(title, desc, price, stock, category)
        elif choice == "2":
            category = input("Categoría: ")
            min_price = int(input("Precio mínimo: "))
            max_price = int(input("Precio máximo: "))
            results = model.get_products_by_category_price(category, min_price, max_price)
            for p in results:
                print(p)
        elif choice == "3":
            keyword = input("Palabra clave: ")
            min_price = int(input("Precio mínimo: "))
            max_price = int(input("Precio máximo: "))
            results = model.search_products_by_keyword(keyword, min_price, max_price)
            for p in results:
                print(p)
        elif choice == "4":
            pid = input("ID de producto: ")
            for r in model.get_reviews_for_product(pid):
                print(r)
        elif choice == "5":
            pid = input("ID del producto: ")
            uid = input("ID del usuario: ")
            rating = int(input("Calificación (1-5): "))
            comment = input("Comentario: ")
            model.add_review(pid, uid, rating, comment)
        elif choice == "6":
            min_rating = float(input("Rating mínimo: "))
            results = model.get_top_rated_products(min_rating)
            for p in results:
                print(p)
        elif choice == "7":
            results = model.get_popular_products()
            for t in results:
                print(t)
        elif choice == "8":
            category = input("Categoría: ")
            results = model.get_products_with_reviews(category)
            for p in results:
                print(p)
        
        elif choice == "9":
            from loader import load_products_csv, load_reviews_csv, load_trending_csv
            load_products_csv("products.csv")
            load_reviews_csv("reviews.csv")
            load_trending_csv("trending.csv")
            print("Datos cargados desde CSV correctamente.")

        elif choice == "0":
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    create_indexes()
    main_menu()
