from db import db
from bson.objectid import ObjectId
from datetime import datetime

def add_product(title, desc, price, stock, category):
    db.products.insert_one({
        "title": title,
        "description": desc,
        "price": price,
        "stock": stock,
        "category": category
    })

def get_products_by_category_price(category, min_price, max_price):
    return list(db.products.find({
        "category": category,
        "price": {"$gte": min_price, "$lte": max_price}
    }))

def search_products_by_keyword(keyword, min_price, max_price):
    return list(db.products.find({
        "$text": {"$search": keyword},
        "price": {"$gte": min_price, "$lte": max_price}
    }))

def get_products_with_reviews(category):
    return list(db.products.aggregate([
        {"$match": {"category": category}},
        {"$lookup": {
            "from": "reviews",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "reviews"
        }}
    ]))

def get_top_rated_products(min_rating):
    return list(db.reviews.aggregate([
        {"$group": {
            "_id": "$product_id",
            "avg_rating": {"$avg": "$rating"}
        }},
        {"$match": {"avg_rating": {"$gt": min_rating}}},
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "_id",
            "as": "product"
        }},
        {"$unwind": "$product"},
        {"$project": {
            "_id": "$product._id",
            "title": "$product.title",
            "description": "$product.description",
            "price": "$product.price",
            "category": "$product.category",
            "rating": "$avg_rating"
        }}
    ]))

def add_review_by_title_and_username(product_title, username, rating, comment):
    product = db.products.find_one({"title": product_title})
    if not product:
        return False
    db.reviews.insert_one({
        "product_id": product["_id"],
        "username": username,
        "rating": rating,
        "comment": comment,
        "created_at": datetime.utcnow()
    })
    return True

def get_reviews_by_product_title(product_title):
    product = db.products.find_one({"title": product_title})
    if not product:
        return []
    return list(db.reviews.find({"product_id": product["_id"]}))

def get_popular_products():
    pipeline = [
        {"$sort": {"last_week_score": -1}},
        {"$limit": 10},
        {"$lookup": {
            "from": "products",
            "localField": "product_id",
            "foreignField": "_id",      
            "as": "product_info"
        }},
        {"$unwind": "$product_info"},
        {"$project": {
            "_id": 1,
            "product_id": 1,
            "views": 1,
            "sales": 1,
            "last_week_score": 1,
            "product_title": "$product_info.title"
        }}
    ]
    
    # Ejecutar el aggregation pipeline
    return list(db.trending.aggregate(pipeline))

def get_product_by_title(title):
    return db.products.find_one({"title": title},{"_id": 0, "title": 1, "description": 1, "price": 1, "stock": 1, "category": 1})

def get_user_by_username(username):
    return db.users.find_one({"username": username})

def print_all_products():
    print("=== Catálogo de Productos ===")
    for product in db.products.find({}, {"title": 1, "price": 1, "category": 1, "_id": 0}):
        print(f"• {product['title']} - ${product['price']} ({product['category']})")
    print()