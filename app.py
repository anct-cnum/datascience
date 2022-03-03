from flask import Flask
from flask_apscheduler import APScheduler

app = Flask(__name__)


class Config:
    """App configuration."""

    JOBS = [
        {
            "id": "job1",
            "func": "jobs:job1",
            "args": (1, 2),
            "trigger": "cron",
            "minute": "*/15"
        }
    ]

    SCHEDULER_API_ENABLED = True


@app.route('/test')
def index():
    return 'Hello, World!'


def job1(var_one, var_two):
    """Demo job function.
    :param var_two:
    :param var_two:
    """
    print(str(var_one) + " " + str(var_two))


if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler = APScheduler()
    scheduler.init_app(app)
    scheduler.start()
    app.run(host='0.0.0.0', port=8080)
