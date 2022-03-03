from flask import Flask
from flask_apscheduler import APScheduler

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


if __name__ == '__main__':
    #scheduler = APScheduler.BackgroundScheduler(timezone="Europe/Berlin")
    #scheduler.init_app(app)
    #scheduler.add_job(test, trigger='cron', minute='*/10')
    #scheduler.start()
    app.run()
