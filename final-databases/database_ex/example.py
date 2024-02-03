import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="root",
  database="mydatabase"
)

mycursor = mydb.cursor()

#mycursor.execute("CREATE DATABASE mydatabase")
#mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")
mycursor.execute("SELECT * FROM mydatabase.customers")

for value in mycursor:
    print(value)
