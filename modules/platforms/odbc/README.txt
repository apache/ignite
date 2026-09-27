## Using pyodbc
GridGain can be used with pyodbc. Here is how you can use pyodbc in GridGain 9:

+ Install pyodbc

pip3 install pyodbc

+ Import pyodbc to your project:

import pyodbc

+ Connect to the database:

conn = pyodbc.connect('Driver={Apache Ignite};Address=127.0.0.1:10800;')

+ Set encoding to UTF-8:

conn.setencoding(encoding='utf-8')
conn.setdecoding(sqltype=pyodbc.SQL_CHAR, encoding="utf-8")
conn.setdecoding(sqltype=pyodbc.SQL_WCHAR, encoding="utf-8")

+ Get data from your database:

cursor = conn.cursor()
cursor.execute('SELECT * FROM table_name')


## Download Linux Platform

https://www.gridgain.com/tryfree#odbcDriver
