import sqlite3
import random
import string


def get_random_alphanumeric_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    result_str = "".join((random.choice(letters_and_digits) for i in range(length)))
    return result_str


conn = sqlite3.connect("access.db")
create_table_query = "CREATE TABLE IF NOT EXISTS access (key text PRIMARY KEY);"
cursor = conn.cursor()
cursor.execute(create_table_query)
api_key = get_random_alphanumeric_string(100)
add_query = "INSERT INTO access(key) VALUES (?);"
cursor.execute(add_query, (api_key,))
conn.commit()
with open("api_secret.txt", "w") as secret:
    secret.write(api_key)
print(f"Api Key Created : {api_key}")
print("This needs to passed when you want to access the API endpoints for scheduling")
cursor.close()
conn.close()
