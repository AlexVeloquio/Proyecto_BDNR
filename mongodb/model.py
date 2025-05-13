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

def add_review(product_id, user_id, rating, comment):
    db.reviews.insert_one({
        "product_id": ObjectId(product_id),
        "user_id": ObjectId(user_id),
        "rating": rating,
        "comment": comment,
        "created_at": datetime.utcnow()
    })

def get_reviews_for_product(product_id):
    return list(db.reviews.find({"product_id": ObjectId(product_id)}))

def get_popular_products():
    return list(db.trending.find().sort("last_week_score", -1).limit(10))

