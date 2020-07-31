from flask import Flask, request
from flask_restful import Resource, Api
import sqlite3

from MoneyControl import money_control_latest_ca_scrape as mc_scrape
from NSE import nse_latest_ca_scrape as nse_scrape
from BSE import bse_latest_ca_scrape as bse_scrape

app = Flask(__name__)
api = Api(app)

conn = None
try:
    conn = sqlite3.connect("access.db")
    cursor = conn.cursor()
    select_query = "SELECT * from access LIMIT 1"
    cursor.execute(select_query)
except:
    print("Cannot find access.db....Please run setup.sh")
    exit()

API_KEY = None

for data in cursor:
    API_KEY = data[0]

if API_KEY is None:
    print("No API Key Present....Please run setup.sh")
    exit()


class MoneyControl(Resource):
    def get(self):
        key = request.args.get("key", None)
        if not key:
            return {"error": "Key not provided"}
        if key != API_KEY:
            return {"error": "Key is invalid"}
        return {"status": mc_scrape.add_to_db()}


class NSE(Resource):
    def get(self):
        key = request.args.get("key", None)
        if not key:
            return {"error": "Key not provided"}
        if key != API_KEY:
            return {"error": "Key is invalid"}
        return {"status": nse_scrape.add_to_db()}


class BSE(Resource):
    def get(self):
        key = request.args.get("key", None)
        if not key:
            return {"error": "Key not provided"}
        if key != API_KEY:
            return {"error": "Key is invalid"}
        return {"status": bse_scrape.latest_ca_scrape()}


api.add_resource(MoneyControl, "/mc")
api.add_resource(NSE, "/nse")
api.add_resource(BSE, "/bse")

if __name__ == "__main__":
    app.run(debug=True)
