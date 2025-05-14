from pymongo import MongoClient, TEXT

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

# Crear colecciones con índices
def create_indexes():
    db.products.create_index([("category", 1)])
    db.products.create_index([("price", 1), ("category", 1)])
    db.products.create_index([("title", TEXT), ("description", TEXT)])
    db.reviews.create_index([("product_id", 1)])
    db.trending.create_index([("last_week_score", -1)])