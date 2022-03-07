# -*- coding: utf-8 -*-
"""

"""

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='',
                                         user='root',
                                         password='')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Serverversion ", db_Info)

       #mySql_insert_Query = """INSERT INTO titleauthors (titleID, auID, importance) VALUES (%s, %s, %s)"""

        #records_to_insert = [ 
      #  (1001,104,1),
       # (1002,105,1),
       # (1003,106,1),
        #(1004,101,1)
       # (1005,103,1),
        #(1005,102,2)]
        #]
        #cursor = connection.cursor()

       # cursor.executemany(mySql_insert_Query, records_to_insert)
        #connection.commit()

        #Query 1
        query = "select * from publishers"
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)
        print("Finished query1\n")

        #Query 2
        create_Table_Query3 = """CREATE TABLE customer ( 
                            custID int(5) NOT NULL,
                            custName varchar(50) NULL,
                            zip varchar(5) NULL,
                            city varchar(30) NULL,
                            state varchar(30) NULL,
                            PRIMARY KEY (custID))"""
        cursor = connection.cursor()
        result = cursor.execute(create_Table_Query3)
        print("Created Customer table")
        print("Finished query2\n")

        #Query 3
        insert_Query3 = """INSERT IGNORE INTO customer (custID, custName, zip, city, state) VALUES (%s, %s, %s, %s, %s)"""

        records_to_insert3 = [ 
       (0, 'STEPHEN WALTHER', 'NULL', 'NULL', 'NULL'), 
       (1,'JAMES GOODWILL', 'NULL', 'NULL', 'NULL'), 
       (2,'CALVIN HARRIS', 'NULL', 'NULL', 'NULL'),
       (3,'MARTIN GARRIX', 'NULL', 'NULL', 'NULL'),
       (4,'PAMELA REIF', 'NULL', 'NULL', 'NULL')]

        cursor = connection.cursor()
        cursor.executemany(insert_Query3, records_to_insert3)
        connection.commit()

        query3 = "select * from customer"
        cursor = connection.cursor()
        cursor.execute(query3)
        for row in cursor.fetchall():
            print(row)
        print("Finished query3\n")

        #Query 4
        query4 = "select aName from authors, customer where aName = custName"
        cursor = connection.cursor()
        cursor.execute(query4)
        for row in cursor.fetchall():
            print(row)
        print("Finished query4\n")

        #Query 5
        query5 = "select sName from subjects as S, titles as T where S.subID = T.subID and T.price >= '400' and T.price <= '550'"
        cursor = connection.cursor()
        cursor.execute(query5)
        for row in cursor.fetchall():
            print(row)
        print("Finished query5\n")
        
        #Query 6
        query6 = "select price from titles as t1 where t1.pubDate = (select max(pubDate) as maxpubDate from titles)"
        cursor = connection.cursor()
        cursor.execute(query6)
        container = []
        for row in cursor.fetchall():
            container.append(row[0])

        for x in container:
            x = int(container[0])
            print(x)
            update_query6 = "UPDATE titles SET price = %s where titleID = %s"
            inputData = (x, '1001')
            cursor.execute(update_query6, inputData)
            connection.commit()

        query6 = "select * from titles"
        cursor = connection.cursor()
        cursor.execute(query6)
        for row in cursor.fetchall():
            print(row)
        print("Finished query6\n")

        #Query 7
        query7 = "select title from titles as T natural join publishers as P where P.pname like '%t%' "
        cursor = connection.cursor()
        cursor.execute(query7)
        for row in cursor.fetchall():
            print(row)
        print("Finished query7\n")

        #Query 8
        query8_1 = "select titleID, auID from titles natural join authors where title = 'JAVA COMP. REF' and aName = 'DAVAID HUNTER'"
        cursor = connection.cursor()
        cursor.execute(query8_1)
        container = []
        for row in cursor.fetchall():
            container.append(row)
        
        title_ID, au_ID = container[0]

        print(title_ID,"   ", au_ID)
        insert_Query8 = """INSERT IGNORE INTO titleauthors (titleID, auID, importance) VALUES (%s, %s, %s)"""
        records = (title_ID, au_ID, 'NULL')
        cursor = connection.cursor()
        cursor.execute(insert_Query8, records)
        connection.commit()

        query8 = "select * from titleauthors"
        cursor = connection.cursor()
        cursor.execute(query8)
        for row in cursor.fetchall():
            print(row)
        print("Finished query8\n")

        #Query 9
        #query9 = "select aName from authors where aName = 'HERBERT SCHILD'"
        #query9 = "select aName from authors as a1 where not exists (select aName from authors as a2 where a2.aName = 'HERBERT SCHILD')"
        query9 = "select aName from authors natural join titleauthors as tA where tA.titleID = (select tA2.titleID from authors natural join titleauthors as tA2 where tA2.auID = '101')"
        cursor = connection.cursor()
        cursor.execute(query9)
        for row in cursor.fetchall():
            print(row)
        print("Finished query9\n")

        #Query 10
        query10_1 = "select price from titles  where pubDate <= '2004-01-01'"
        cursor = connection.cursor()
        cursor.execute(query10_1)
        container = []
        for row in cursor.fetchall():
            container.append(row[0])
            
        i = 0

        for x in container:
            x = float(container[i])
            
            query10 = "Update titles set price = %s where pubDate <= '2004-01-01' and price = %s" 
            price = x 
            x *= .7
            i += 1
            input= (x,price)
            cursor.execute(query10, input)
            connection.commit()
            #print(x)  

        query10_2 = "select price from titles  where pubDate > '2004-01-01'"
        cursor = connection.cursor()
        cursor.execute(query10_2)
        container2 = []
        for row2 in cursor.fetchall():
            container2.append(row2[0])

        i2 = 0
        
        for x in container2:
            x = float(container2[i2])
            
            query10_2 = "Update titles set price = %s where pubDate > '2004-01-01' and price = %s" 
            price2 = x 
            x *= .85
            i2 += 1
            input2= (x,price2)
            cursor.execute(query10_2, input2)
            connection.commit()
        
        query10 = "select * from titles"
        cursor = connection.cursor()
        cursor.execute(query10)
        for row in cursor.fetchall():
            print(row)
        print("Finished query10\n")

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")