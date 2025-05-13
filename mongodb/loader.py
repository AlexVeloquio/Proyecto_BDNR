import csv
from bson.objectid import ObjectId
from datetime import datetime
from db import db
from model import add_product

def load_products_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            add_product(row['title'], row['description'], int(row['price']), int(row['stock']), row['category'])

def load_reviews_csv(path):
    title_to_id = {p["title"]: p["_id"] for p in db.products.find()}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = title_to_id.get(row['product_title'])
            if product_id:
                db.reviews.insert_one({
                    "product_id": product_id,
                    "user_id": ObjectId(row["user_id"]),
                    "rating": int(row["rating"]),
                    "comment": row["comment"],
                    "created_at": datetime.fromisoformat(row["created_at"])
                })

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
                    "last_week_score": int(row["last_week_score"])
                })
