from datetime import datetime
from configparser import ConfigParser
import psycopg2

configure = ConfigParser()
configure.read("secret.ini")

DATABASE = configure.get("POSTGRES", "DATABASE")
USER = configure.get("POSTGRES", "USER")
PASSWORD = configure.get("POSTGRES", "PASSWORD")
HOST = configure.get("POSTGRES", "HOST")
PORT = configure.get("POSTGRES", "PORT")


def latest_ca():
    connection = psycopg2.connect(
        user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE,
    )
    cursor = connection.cursor()
    query = f'SELECT * FROM latest_nse_ca'
    cursor.execute(query)
    ca_array = []
    for data in cursor:
        corporate_action = {
            'symbol': data[1],
            'company_name': data[2],
            'series': data[3],
            'face_value': data[4],
            'purpose': data[5],
            'ex_date': data[6].strftime("%d-%b-%Y"),
            'record_date': data[7],
            'bc_start_date': data[8],
            'bc_end_date': data[9],
        }
        ca_array.append(corporate_action)
    connection.commit()
    cursor.close()
    connection.close()
    return ca_array