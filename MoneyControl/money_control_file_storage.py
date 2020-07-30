import pandas as pd
import base64
import os
import pdfkit
import boto3

from configparser import ConfigParser

configure = ConfigParser()
configure.read("secret.ini")

ACCESS_KEY = configure.get("AWS", "ACCESS_KEY")
SECRET_KEY = configure.get("AWS", "SECRET_KEY")
BUCKET = configure.get("AWS", "BUCKET")

from MoneyControl.money_control_get import latest_ca

def store_file_as_csv_pdf():
    filename = "Latest Corporate Actions"
    get_latest_ca = latest_ca()
    ret1 = store_file(f"{filename} MC.csv", get_latest_ca)
    ret2 = store_file(f"{filename} MC.pdf", get_latest_ca, 'pdf')
    return ret1 and ret2


def store_file(filename, data, typ="csv"):
    if typ == 'csv':
        df=pd.DataFrame(data=data)
        df.index+=1
        file_path = os.path.join(filename)
        df.to_csv(file_path)
    elif typ == 'pdf':
        df=pd.DataFrame(data=data)
        directory = os.path.dirname(os.path.realpath(__file__))
        html_file_path = os.path.join("LatestCorporateActions.html")
        pdf_file_path = os.path.join(filename)
        fd = open(html_file_path,'w')
        intermediate = df.to_html()
        fd.write(intermediate)
        fd.close()
        pdfkit.from_file(html_file_path, pdf_file_path)
    uploaded = upload_to_aws(filename, filename)
    return uploaded

def upload_to_aws(local_file, s3_file):
    bucket = BUCKET
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False