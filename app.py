from flask import Flask
from flask_apscheduler import APScheduler
import pandas as pd
from bson import ObjectId
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from src.mongodb import connect_db_prod
from src.data.retrieveData import create_dataframe_prod

app = Flask(__name__)
scheduler = APScheduler()


class Config:
    """App configuration."""
    SCHEDULER_API_ENABLED = True


@app.route('/test')
def index():
    return 'Hello, World!'

# interval examples
@scheduler.task("interval", id="do_job_1", seconds=30, misfire_grace_time=900)
def job1():
    """Sample job 1."""
    print("Job 1 executed")


# cron examples
@scheduler.task("cron", id="do_job_2", minute="*")
def job2():
    """Sample job 2."""
    print("Job 2 executed")


@scheduler.task("cron", id="do_job_3", week="*", day_of_week="sun")
def job3():
    """Sample job 3."""
    print("Job 3 executed")


if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler.init_app(app)
    scheduler.start()
    app.run()
