from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from bson import ObjectId
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from src.mongodb import connect_db_prod
from src.data.retrieveData import create_dataframe_prod

app = Flask(__name__)


@app.route('/test')
def index():
    return 'Hello, World!'


def test():
    print("allo")


scheduler = BackgroundScheduler(timezone="Europe/Berlin")
job = scheduler.add_job(test, trigger='cron', minute='*/10')
scheduler.start()


if __name__ == '__main__':
    app.run()
