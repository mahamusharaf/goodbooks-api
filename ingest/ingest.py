import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
import numpy as np


# MongoDB config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "goodbooks")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

DATASET = os.getenv("DATASET", "")  # empty means use 'data/' directly
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", DATASET)

# Mapping collection -> unique keys
UNIQUE_KEYS = {
    "books": ["book_id"],
    "ratings": ["user_id", "book_id"],
    "tags": ["tag_id"],
    "book_tags": ["goodreads_book_id", "tag_id"],
    "to_read": ["user_id", "book_id"]
}

def load_csv_to_mongo(filename, collection_name, chunksize=50000):
    filepath = os.path.join(DATA_PATH, filename)
    print(f"📥 Loading {filename} into collection '{collection_name}'...")

    total_upserted = 0
    for chunk in pd.read_csv(filepath, chunksize=chunksize):
        chunk = chunk.fillna(value=np.nan)  # Handle NaNs
        operations = []
        keys = UNIQUE_KEYS.get(collection_name, ["_id"])
        for record in chunk.to_dict("records"):
            filter_doc = {k: record[k] for k in keys}
            operations.append(UpdateOne(filter_doc, {"$set": record}, upsert=True))
        if operations:
            result = db[collection_name].bulk_write(operations)
            total_upserted += result.upserted_count + result.modified_count

    print(f"✅ Finished {collection_name}: Upserted {total_upserted} records")

if __name__ == "__main__":
    load_csv_to_mongo("books.csv", "books")
    load_csv_to_mongo("ratings.csv", "ratings")
    load_csv_to_mongo("tags.csv", "tags")
    load_csv_to_mongo("book_tags.csv", "book_tags")
    load_csv_to_mongo("to_read.csv", "to_read")
    print("🎉 All datasets ingested successfully!")
