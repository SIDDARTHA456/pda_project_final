from pymongo import MongoClient
from config import Config

client = MongoClient(Config.MONGO_URI)
db = client[Config.DATABASE_NAME]

raw_collection = db[Config.RAW_COLLECTION]
processed_collection = db[Config.PROCESSED_COLLECTION]