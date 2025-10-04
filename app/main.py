import os

from fastapi import FastAPI, Query, HTTPException, Body,Header
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId

app = FastAPI(title="GoodBooks API")
API_KEY = os.getenv("API_KEY", "secret123")
class Rating(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(..., ge=1, le=5)

client = MongoClient("mongodb://localhost:27017")
db = client.goodbooks

def serialize_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc

def serialize_docs(docs):
    return [serialize_doc(doc) for doc in docs]

def paginate(cursor, page=1, page_size=20, filter={}):

    items = list(cursor.skip((page-1)*page_size).limit(page_size))
    total = cursor.collection.count_documents(filter)  # safe counting
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": serialize_docs(items)
    }

@app.get("/books")
def list_books(
        q: str | None = None,
        tag: str | None = None,
        min_avg: float | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        sort: str = Query("avg", pattern="^(avg|ratings_count|year|title)$"),
        order: str = Query("desc", pattern="^(asc|desc)$"),
        page: int = 1,
        page_size: int = Query(20, le=100)
):
    filt = {}

    if q:
        filt["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]

    if min_avg is not None:
        filt["average_rating"] = {"$gte": float(min_avg)}

    year_filter = {}
    if year_from is not None:
        year_filter["$gte"] = year_from
    if year_to is not None:
        year_filter["$lte"] = year_to
    if year_filter:
        filt["original_publication_year"] = year_filter

    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count",
        "year": "original_publication_year",
        "title": "title"
    }
    direction = -1 if order == "desc" else 1

    total = db.books.count_documents(filt)
    items = list(db.books.find(filt)
                 .sort([(sort_map[sort], direction)])
                 .skip((page-1)*page_size)
                 .limit(page_size))

    return {
        "items": serialize_docs(items),
        "page": page,
        "page_size": page_size,
        "total": total
    }


@app.get("/books/{book_id}")
def get_book(book_id: str):
    try:
        obj_id = ObjectId(book_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid book ID")

    book = db.books.find_one({"_id": obj_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return serialize_doc(book)


@app.get("/books/{book_id}/tags")
def get_book_tags(book_id: int):
    pipeline = [
        {"$match": {"goodreads_book_id": book_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "tags"
        }},
        {"$unwind": "$tags"},
        {"$replaceRoot": {"newRoot": "$tags"}}
    ]
    tags = list(db.book_tags.aggregate(pipeline))
    return {"items": serialize_docs(tags), "total": len(tags)}


@app.get("/authors/{author_name}/books")
def get_author_books(author_name: str, page: int = 1, page_size: int = Query(20, le=100)):
    filter = {"authors": {"$regex": author_name, "$options": "i"}}
    cursor = db.books.find(filter)
    return paginate(cursor, page, page_size, filter=filter)

@app.get("/tags")
def list_tags(page: int = 1, page_size: int = Query(20, le=100)):
    pipeline = [
        {"$lookup": {
            "from": "book_tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "books"
        }},
        {"$addFields": {"book_count": {"$size": "$books"}}}
    ]
    tags = list(db.tags.aggregate(pipeline))
    total = len(tags)
    start = (page-1)*page_size
    end = start+page_size
    return {
        "items": serialize_docs(tags[start:end]),
        "page": page,
        "page_size": page_size,
        "total": total
    }

@app.get("/users/{user_id}/to-read")
def user_to_read(user_id: int, page: int = 1, page_size: int = Query(20, le=100)):
    filter = {"user_id": user_id}
    cursor = db.to_read.find(filter)
    return paginate(cursor, page, page_size, filter=filter)

@app.get("/books/{book_id}/ratings/summary")
def book_ratings_summary(book_id: int):
    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": "$book_id",
            "average": {"$avg": "$rating"},
            "count": {"$sum": 1},
            "histogram": {"$push": "$rating"}
        }}
    ]
    result = list(db.ratings.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No ratings found")

    res = result[0]
    histogram = {i: res["histogram"].count(i) for i in range(1, 6)}
    return {
        "average": res["average"],
        "count": res["count"],
        "histogram": histogram
    }

@app.post("/ratings")
def add_rating(rating: Rating, x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    return {"message": "Rating added/updated successfully"}
