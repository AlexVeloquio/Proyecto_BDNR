import csv
from bson.objectid import ObjectId
from datetime import datetime
from db import db
from model import add_product

def load_products_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            add_product(row['title'], row['description'], float(row['price']), int(row['stock']), row['category'])

def load_reviews_csv(path):
    title_to_id = {p["title"].strip().lower(): p["_id"] for p in db.products.find()}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_title_key = row['product_title'].strip().lower()
            product_id = title_to_id.get(product_title_key)
            if product_id:
                db.reviews.insert_one({
                    "product_id": product_id,
                    "username": row["username"],
                    "rating": float(row["rating"]),
                    "comment": row["comment"],
                    "created_at": datetime.fromisoformat(row["created_at"])
                })
            else:
                print(f"[AVISO] Producto no encontrado: '{row['product_title']}'")


def load_trending_csv(path):
    title_to_id = {p["title"]: p["_id"] for p in db.products.find()}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = title_to_id.get(row['product_title'])
            if product_id:
                db.trending.insert_one({
                    "product_id": product_id,
                    "views": int(row["views"]),
                    "sales": int(row["sales"]),
                    "last_week_score": float(row["last_week_score"])
                })
