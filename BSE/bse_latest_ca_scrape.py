import requests
import json
from bs4 import BeautifulSoup as soup
import os
from selenium import webdriver
import sqlite3
from datetime import datetime
import psycopg2
from configparser import ConfigParser

from BSE.bse_file_storage import store_file_as_csv_pdf

configure = ConfigParser()
configure.read("secret.ini")

DATABASE = configure.get("POSTGRES", "DATABASE")
USER = configure.get("POSTGRES", "USER")
PASSWORD = configure.get("POSTGRES", "PASSWORD")
HOST = configure.get("POSTGRES", "HOST")
PORT = configure.get("POSTGRES", "PORT")


def latest_ca_scrape():
    res = requests.get(
        "https://www.bseindia.com/corporates/corporate_act.aspx",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    res.raise_for_status()
    page_soup = soup(res.content, features="lxml")

    no_of_pages_tab = page_soup.find("tr", {"class": "pgr"})
    if no_of_pages_tab == None:
        no_of_pages = 1
    else:
        no_of_pages = len(no_of_pages_tab.find_all("a")) + 1

        options = webdriver.ChromeOptions()
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--incognito")
        # This prevents the browser from opening up
        options.add_argument("--headless")
        driver = webdriver.Chrome(chrome_options=options)

    pageSource = res.content
    dataList = []
    page_soup = soup(pageSource, features="lxml")
    dataRows = page_soup.find_all("tr", {"class": "TTRow"})

    # Scraping the data and adding it into the dataList
    for dataRow in dataRows:
        dataColumns = dataRow.find_all("td")
        data = []
        for dataColumn in dataColumns:
            data.append(dataColumn.text)
        dataList.append(data)

    if no_of_pages > 1:
        print("Entered first if")

        for i in range(2, no_of_pages + 1):
            print("Entered ", i)
            xpath = f'//*[@id="ContentPlaceHolder1_gvData"]/tbody/tr[1]/td/table/tbody/tr/td[{i}]/a'
            print(xpath)
            driver.get("https://www.bseindia.com/corporates/corporate_act.aspx")
            driver.find_element_by_xpath(xpath).click()
            pageSource = driver.page_source
            page_soup = soup(pageSource, features="lxml")
            dataRows = page_soup.find_all("tr", {"class": "TTRow"})
            for dataRow in dataRows:
                dataColumns = dataRow.find_all("td")
                data = []
                for dataColumn in dataColumns:
                    data.append(dataColumn.text)
                dataList.append(data)

    # Initializing the database
    dataList = dataList
    connection = psycopg2.connect(
        user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE,
    )
    cursor = connection.cursor()
    cursor_new = connection.cursor()
    create_table = "CREATE TABLE IF NOT EXISTS latest_bse_ca (key text PRIMARY KEY UNIQUE,security_code text, security_name text, ex_date DATE, purpose text, record_date text,bc_start_date text,bc_end_date text,nd_start_date text,nd_end_date text,actual_payment_date text)"
    cursor.execute(create_table)
    connection.commit()

    # Transfering the data of the latest corporate action to the storage
    create_table = "CREATE TABLE IF NOT EXISTS bse_ca (key text PRIMARY KEY UNIQUE,security_code text, security_name text, ex_date DATE, purpose text, record_date text,bc_start_date text,bc_end_date text,nd_start_date text,nd_end_date text,actual_payment_date text)"
    cursor_new.execute(create_table)
    connection.commit()
    cursor_new.execute("SELECT * FROM latest_bse_ca")
    add_data_to_db = "INSERT INTO bse_ca VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for data in cursor_new:
        try:
            cursor.execute(
                add_data_to_db,
                (
                    data[0],
                    data[1],
                    data[2],
                    data[3],
                    data[4],
                    data[5],
                    data[6],
                    data[7],
                    data[8],
                    data[9],
                    data[10],
                ),
            )
        except:
            # print("Skipped")
            pass
    # Deleting the preexisting data from the database
    connection.commit()
    cursor.execute("DELETE FROM latest_bse_ca")
    connection.commit()
    cursor_new.close()

    # Adding data to the database
    add_data_to_db = (
        "INSERT INTO latest_bse_ca VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    for data in dataList:
        try:
            datetime.strptime(data[2], "%d %b %Y")
        except:
            continue
        # print(data[2])
        uniqueKey = data[0] + data[2] + data[3]
        cursor.execute(
            add_data_to_db,
            (
                uniqueKey,
                data[0],
                data[1],
                datetime.strptime(data[2], "%d %b %Y").strftime("%Y-%m-%d"),
                data[3],
                data[4],
                data[5],
                data[6],
                data[7],
                data[8],
                data[9],
            ),
        )
    connection.commit()
    cursor.close()
    connection.close()
    print("latest corporate action database updated successfully")
    ret = store_file_as_csv_pdf()
    return ret
