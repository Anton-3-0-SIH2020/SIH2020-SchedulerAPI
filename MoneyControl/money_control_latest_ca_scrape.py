import requests
import json
from bs4 import BeautifulSoup as soup
from datetime import datetime
from configparser import ConfigParser
from MoneyControl.money_control_file_storage import store_file_as_csv_pdf
import psycopg2

configure = ConfigParser()
configure.read("secret.ini")

DATABASE = configure.get("POSTGRES", "DATABASE")
USER = configure.get("POSTGRES", "USER")
PASSWORD = configure.get("POSTGRES", "PASSWORD")
HOST = configure.get("POSTGRES", "HOST")
PORT = configure.get("POSTGRES", "PORT")


def money_control_ca_scraper():
    ca_list = []

    res = requests.get(
        "https://www.moneycontrol.com/stocks/marketinfo/upcoming_actions/home.html"
    )
    res.raise_for_status()
    page_soup = soup(res.content, features="lxml")

    div = page_soup.find_all("div", {"class": "tbldata36 PT20"})
    bonus_div = page_soup.find("div", {"class": "tbldata36 PT10"})
    splits_div = div[0]
    rights_div = div[1]
    dividends_div = div[2]

    def check_if_div_empty(temp_div):
        td = temp_div.find_all("td", {"style": "text-align:center; border-left:none"})
        if len(td) == 1:
            return True
        return False

    def splits_scraper(temp_div):
        tr_list = temp_div.find_all("tr")
        no_of_items = len(tr_list)
        for i in range(1, no_of_items):
            td_list = tr_list[i].find_all("td")
            ca = []
            ca.append(td_list[0].getText())
            ca.append(
                "SPLITS from Old FV "
                + td_list[1].getText()
                + " to New FV "
                + td_list[2].getText()
            )
            ca.append("None")
            ca.append("None")
            ca.append(td_list[3].getText())
            ca_list.append(ca)

    def dividends_scraper(temp_div):
        tr_list = temp_div.find_all("tr")
        no_of_items = len(tr_list)
        for i in range(1, no_of_items):
            td_list = tr_list[i].find_all("td")
            ca = []
            ca.append(td_list[0].getText())
            ca.append(td_list[1].getText() + " DIVIDEND of " + td_list[2].getText())
            ca.append(td_list[3].getText())
            ca.append(td_list[4].getText())
            ca.append(td_list[5].getText())
            ca_list.append(ca)

    def bonus_scraper(temp_div):
        tr_list = temp_div.find_all("tr")
        no_of_items = len(tr_list)
        for i in range(1, no_of_items):
            td_list = tr_list[i].find_all("td")
            ca = []
            ca.append(td_list[0].getText())
            ca.append("BONUS RATIO of " + td_list[1].getText())
            ca.append(td_list[2].getText())
            ca.append(td_list[3].getText())
            ca.append(td_list[4].getText())
            ca_list.append(ca)

    def rights_scraper(temp_div):
        tr_list = temp_div.find_all("tr")
        no_of_items = len(tr_list)
        for i in range(1, no_of_items):
            td_list = tr_list[i].find_all("td")
            ca = []
            ca.append(td_list[0].getText())
            ca.append(
                "RIGHTS RATIO of "
                + td_list[1].getText()
                + " with PREMIUM of "
                + td_list[2].getText()
            )
            ca.append(td_list[3].getText())
            ca.append(td_list[4].getText())
            ca.append(td_list[5].getText())
            ca_list.append(ca)

    bonus_div_is_empty = check_if_div_empty(bonus_div)
    splits_div_is_empty = check_if_div_empty(splits_div)
    rights_div_is_empty = check_if_div_empty(rights_div)
    dividends_div_is_empty = check_if_div_empty(dividends_div)

    if not splits_div_is_empty:
        splits_scraper(splits_div)
    if not dividends_div_is_empty:
        dividends_scraper(dividends_div)
    if not rights_div_is_empty:
        rights_scraper(rights_div)
    if not bonus_div_is_empty:
        bonus_scraper(bonus_div)

    # print(ca_list)
    return ca_list


def add_to_db():
    ca_list = money_control_ca_scraper()
    # Initializing the database
    connection = psycopg2.connect(
        user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE,
    )
    cursor = connection.cursor()
    cursor_new = connection.cursor()
    create_table = "CREATE TABLE IF NOT EXISTS latest_mc_ca (key text PRIMARY KEY UNIQUE, company_name text, purpose text, anouncment_date text, record_date text, ex_date DATE)"
    cursor.execute(create_table)
    connection.commit()

    # Transfering the data of the latest corporate action to the storage
    create_table = "CREATE TABLE IF NOT EXISTS mc_ca (key text PRIMARY KEY UNIQUE, company_name text, purpose text, anouncment_date text, record_date text, ex_date DATE)"
    cursor_new.execute(create_table)
    connection.commit()
    cursor_new.execute("SELECT * FROM latest_mc_ca")
    add_data_to_db = "INSERT INTO mc_ca VALUES (%s,%s,%s,%s,%s, %s)"
    # print(cursor_new)
    for data in cursor_new:
        try:
            cursor.execute(
                add_data_to_db, (data[0], data[1], data[2], data[3], data[4], data[5])
            )
            connection.commit()
        except:
            connection.rollback()
    cursor_new.execute("DELETE FROM latest_mc_ca")
    connection.commit()
    cursor_new.close()

    # Adding data to the database
    add_data_to_db = "INSERT INTO latest_mc_ca VALUES (%s,%s,%s,%s,%s,%s)"
    for data in ca_list:
        uniqueKey = data[0] + data[1] + data[2]
        try:
            datetime.strptime(data[4], "%d-%m-%Y").strftime("%Y-%m-%d")
        except:
            continue
        cursor.execute(
            add_data_to_db,
            (
                uniqueKey,
                data[0],
                data[1],
                data[2],
                data[3],
                datetime.strptime(data[4], "%d-%m-%Y").strftime("%Y-%m-%d"),
            ),
        )
    connection.commit()
    cursor.close()
    connection.close()
    print("latest corporate action database updated successfully")
    ret = store_file_as_csv_pdf()
    return ret

