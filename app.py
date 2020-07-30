from flask import Flask
from flask_restful import Resource, Api

from MoneyControl import money_control_latest_ca_scrape as mc_scrape
from NSE import nse_latest_ca_scrape as nse_scrape
from BSE import bse_latest_ca_scrape as bse_scrape

app = Flask(__name__)
api = Api(app)


class MoneyControl(Resource):
    def get(self):
        return {"status": mc_scrape.add_to_db()}


class NSE(Resource):
    def get(self):
        return {"status": nse_scrape.add_to_db()}


class BSE(Resource):
    def get(self):
        return {"status": bse_scrape.latest_ca_scrape()}


api.add_resource(MoneyControl, "/mc")
api.add_resource(NSE, "/nse")
api.add_resource(BSE, "/bse")

if __name__ == "__main__":
    app.run(debug=True)
