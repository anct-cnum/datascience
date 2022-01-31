from pymongo import MongoClient, errors
import os
from dotenv import load_dotenv

load_dotenv()


def connect_db():
    try:
        client = MongoClient(port=int(os.environ.get('MONGO_PORT')), host=os.environ.get('MONGO_HOTE'))
        db = client[os.environ.get('MONGO_DATABASE')]

        print("server version:", client.server_info()["version"])

        return db

    except errors.ServerSelectionTimeoutError as err:
        # set the client instance to 'None' if exception
        client = None

        # catch pymongo.errors.ServerSelectionTimeoutError
        print("pymongo ERROR:", err)
