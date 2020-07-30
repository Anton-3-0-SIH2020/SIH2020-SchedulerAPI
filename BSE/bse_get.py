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
    query = f'SELECT * FROM latest_bse_ca'
    cursor.execute(query)
    ca_array = []
    for data in cursor:
        security_name = data[2][1:len(data[2])-1]
        if(data[10] == '\n-\n'):
            actual_payment_data = '-'
        else:
            actual_payment_data = data[10]

        corporate_action = {
            'security_code': data[1],
            'security_name': security_name,
            'ex_date': datetime.strptime(data[3], "%Y-%m-%d").strftime("%d-%b-%Y"),
            'purpose': data[4],
            'record_date': data[5],
            'bc_start_date': data[6],
            'bc_end_date': data[7],
            'nd_start_date': data[8],
            'nd_end_date': data[9],
            'actual_payment_date': actual_payment_data
        }
        ca_array.append(corporate_action)
    connection.commit()
    cursor.close()
    connection.close()
    return (ca_array)