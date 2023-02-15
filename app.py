from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

from common_config import *


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = \
    f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@localhost/{DATABASE}'
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = 'secret string'

# Initialising DB
db = SQLAlchemy(app)

# =============================== Model =======================================
class Weather(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(8))
    max_temp = db.Column(db.Integer)
    min_temp = db.Column(db.Integer)
    precipitation = db.Column(db.Integer)
    station = db.Column(db.String(16))

    def __init__(self, id, date, max_temp, min_temp, precipitation, station):
        self.id = id
        self.date = date
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.precipitation = precipitation
        self.station = station

    def serialize(self):
        return {
            'id':self.id,
            'date': self.date,
            'max_temp': self.max_temp,
            'min_temp': self.min_temp,
            'precipitation' : self.precipitation,
            'station':  self.station
        }


class Crop(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.String(8))
    yield_per_year = db.Column(db.Integer())

    def __init__(self, id, year, yield_per_year):
        self.id = id
        self.year = year
        self.yield_per_year = yield_per_year

    def serialize(self):
        return {
        'id':self.id,
            'year': self.year,
            'yield_per_year': self.yield_per_year
        }

# =========================== Utils ===============================

import os
import pandas as pd


def get_weather_data(path):
    if os.path.isfile(path):
        return pd.read_csv(path, sep='\t', names=['date', 'max_temp', 'min_temp', 'precipitation'])


def get_crop_data(path):
    if os.path.isfile(path):
        return pd.read_csv(path, sep='\t', names=['year', 'yield_per_year'])


# =========================================================================
def get_filename(path):
    filename = os.path.basename(path)
    return os.path.splitext(filename)[0]


def save_weather():
    file_path = WEATHER_FILE_PATH
    res=""
    if os.path.exists(file_path):
        for file in os.listdir(file_path):
            df = get_weather_data(file)
            df['location'] = get_filename(file)
            db_value = df.to_dict(orient='records')
            weather_data = [Weather(**weather) for weather in db_value]
            try:
                db.session.add(weather_data)
                db.session.commit()
                res = 'Successfully added data'
                break
            except Exception as e:
                res = 'Unable to add data'

    return res



def save_crop():
    file_path = CROP_FILE_PATH
    res=""
    if os.path.exists(file_path):
        for file in os.listdir(file_path):
            df = get_crop_data(file)
            db_value = df.to_dict(orient='records')
            crop_data = [Crop(**crop) for crop in db_value]
            try:
                db.session.add(crop_data)
                db.session.commit()
                res= 'Successfully added data'
                break
            except Exception as e:
                res= 'Unable to add data'
    return res


@app.route('/api/weather')
def ingest_weather_data():
    try:
        save_weather()
        data= jsonify({"Success": "Saved weather."})
    except:
        data= jsonify({"Error": "Unable to save weather."})
    return data


@app.route('/api/yield')
def ingest_crop_yield_data():
    try:
        save_crop()
        data = jsonify({"Success": "Saved crop."})
    except:
        data = jsonify({"Error": "Unable to save crop."})
    return data


@app.route('/api/weather/stats')
def get_statistics_data():
    max_temp = (
        db.session.query(Weather).group_by(func.avg(Weather.max_temp))
    )

    min_temp = (
        db.session.query(Weather).group_by(func.avg(Weather.min_temp))
    )
    avg_per = (
        db.session.query(Weather)\
            .group_by(func.sum(Weather.precipitation))
    )

    return jsonify({'max_temp': max_temp, 'min_temp': min_temp, 'total': avg_per})


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
