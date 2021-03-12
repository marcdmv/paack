#1. Can you sort a numerical list in Python?
#Yes, first map all the strings to integers and then using the sorted() function.
list = ["5", "8", "1", "2", "10"]
sorted_list = map(int, list)
sorted_list = sorted(sorted_list)
print(sorted_list)

#2. Write a code to count the number of capital letters in the “drivers_table.csv” file.
count = 0
with open('drivers_table.csv') as countletter:
    text = countletter.read()
    for char in text:
        if char.isupper():
            count += 1
print(count)

#3. Write a function that lists the files in a path with a specific file extension.
from os import listdir

def list_files(directory,extension):
    return (f for f in listdir(directory) if f.endswith('.' + extension))

files = list_files('/Users/marcdemas/git/paack',"csv")
for f in files:
    print(f)

#4. Could you provide a code that executes the query you have created previously in
# question 6 of SQL and export the result to a CSV?
import psycopg2

db_conn = psycopg2.connect(database="paack", user="test", password="", host="127.0.0.1", port="5432")
db_cursor = db_conn.cursor()

s = '''SELECT drivers.id driver_id,
		drivers.driver,
		orders.deliver_date date,
		ROUND((COUNT(orders.paack_order_number)/SUM(EXTRACT(HOUR FROM orders.delivery_end) - EXTRACT(HOUR FROM orders.delivery_start)))::numeric,2) productivity
        FROM drivers
        LEFT JOIN orders ON orders.driver_id = drivers.id
        GROUP BY drivers.id, drivers.driver, orders.deliver_date
        ORDER BY drivers.id ASC'''

sql = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)

output_path = "/Users/marcdemas/git/paack/output.csv"

with open(output_path, 'w') as file:
    db_cursor.copy_expert(sql, file)

db_conn.close()

#5. Can you write a code that executes a query in one database and insert the data
# in a different database?
import csv

db_conn = psycopg2.connect(database="paack", user="test", password="", host="127.0.0.1", port="5432")
db_cursor = db_conn.cursor()
db_conn2 = psycopg2.connect(database="paack2", user="test", password="", host="127.0.0.1", port="5432")
db_cursor2 = db_conn2.cursor()

s = '''SELECT * FROM orders'''
sql = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)

output_path_2 = "/Users/marcdemas/git/paack/output2.csv"

with open(output_path_2, 'w') as file:
    db_cursor.copy_expert(sql, file)
db_cursor2.execute("DROP TABLE orders")
db_conn2.commit()

db_cursor2.execute('''CREATE
    TABLE
    orders
    (id INTEGER,
    paack_order_number INTEGER,
    driver_id INTEGER,
    deliver_date DATE,
    delivery_start timestamp,
    delivery_end timestamp,
    attempted_time timestamp,
    order_status VARCHAR,
    country VARCHAR,
    company VARCHAR)''')
db_conn2.commit()

with open(output_path_2, 'r') as f:
    reader = csv.reader(f)
    next(reader)
    db_cursor2.execute("DROP TABLE orders")
    db_cursor2.execute('''CREATE
    TABLE
    orders
    (id INTEGER,
    paack_order_number INTEGER,
    driver_id INTEGER,
    deliver_date DATE,
    delivery_start timestamp,
    delivery_end timestamp,
    attempted_time timestamp,
    order_status VARCHAR,
    country VARCHAR,
    company VARCHAR)''')
    for row in reader:
        db_cursor2.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

db_conn.close()
db_conn2.commit()
db_conn2.close()