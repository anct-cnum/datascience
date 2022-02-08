from pymongo import MongoClient, errors
import os
from dotenv import load_dotenv

load_dotenv()


def connect_db_datalake():
    try:
        client = MongoClient(port=int(os.environ.get('MONGO_PORT')), host=os.environ.get('MONGO_HOTE'))
        db = client[os.environ.get('MONGO_DATABASE_DATALAKE')]
        print("données récupérer avec succès sur le datalake")

        return db

    except errors.ServerSelectionTimeoutError as err:
        # set the client instance to 'None' if exception
        client = None

        # catch pymongo.errors.ServerSelectionTimeoutError
        print("pymongo ERROR:", err)


def connect_db_prod():
    try:
        client = MongoClient(port=int(os.environ.get('MONGO_PORT')), host=os.environ.get('MONGO_HOTE'))
        db = client[os.environ.get('MONGO_DATABASE_PROD')]
        print("données récupérer avec succès sur la prod")

        return db

    except errors.ServerSelectionTimeoutError as err:
        # set the client instance to 'None' if exception
        client = None

        # catch pymongo.errors.ServerSelectionTimeoutError
        print("pymongo ERROR:", err)
