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
    query = f'SELECT * FROM mc_ca'
    cursor.execute(query)
    ca_array = []
    for data in cursor:
        corporate_action = {
            'company_name': data[1],
            'purpose': data[2],
            'anouncement': data[3],
            'record_date': data[4],
            'ex-date': data[5].strftime("%d-%b-%Y"),
            'bc_start_date': 'None',
            'bc_end_date': 'None',
            'nd_start_date': 'None',
            'nd_end_date': 'None',
            'actual_payment_date': 'None'
        }
        ca_array.append(corporate_action)
    connection.commit()
    cursor.close()
    connection.close()
    return (ca_array)