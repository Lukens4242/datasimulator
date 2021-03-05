#!/usr/local/bin/python3

import psycopg2
import random
import uuid
import datetime 
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from concurrent import futures
import concurrent
import time
import math
from psycopg2.pool import ThreadedConnectionPool

DSN = "postgresql://root@34.74.144.16:26257/test"
max_concurrency = 200 #one thread per table is generated
num_tables = 400 #num of tables to create
num_fields = 15 #num of fields per table
num_rows = 50000000 #num of rows per table; make sure this is evently divisible by the batchsize
batchsize = 100 #number of inserts to do as a single statement

starttime = datetime.datetime.now()
livethreads = 0
numtablesdone = 0
mybatches = 0
totalbatches = num_tables * num_rows / batchsize

def write_table(table_id):
    global livethreads
    global mybatches
    livethreads = livethreads + 1
       
    #build a table sql
    myquery = "CREATE TABLE IF NOT EXISTS table" + str(table_id).zfill(6) + " (field1 UUID PRIMARY KEY default gen_random_uuid(), field000002 INT"
    for i in range(1, num_fields - 2 + 1):
        fieldnum = i + 2
        myquery = myquery + ", field" + str(fieldnum).zfill(6) + " INT"

    myquery = myquery + ");"

    try: 
        conn
    except NameError: 
        conn = tcp.getconn()
        conn.autocommit = True
    else: 
        pass

    try: 
        cursor
    except NameError: 
        cursor = conn.cursor()
    else: 
        pass

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    
    try:
        cursor
    except NameError:
        pass
    else:
        cursor.close()


    try:
        conn
    except NameError:
        pass
    else:
        tcp.putconn(conn, close=False)

    

    #build a one row insert and loop

    try: 
        conn2
    except NameError: 
        conn2 = tcp.getconn()
        conn2.autocommit = True
    else: 
        pass
    
    try: 
        cursor2
    except NameError: 
        cursor2 = conn2.cursor()
    else: 
        pass

    
    for j in range(1, num_rows + 1):
        if (((j-1) % batchsize) == 0):
            myquery = "INSERT INTO table" + str(table_id).zfill(6) + " (field000002"
            for i in range(1, num_fields - 2 + 1):
                fieldnum = i + 2
                myquery = myquery + ",field" + str(fieldnum).zfill(6)
            myquery = myquery +  ") VALUES"
        
        myquery = myquery + "('" + str(j) + "'"
        myrandom = random.randint(1,1000)
        for i in range(1, num_fields - 2  + 1):
            fieldnum = i + 2
            myquery = myquery + ", '" + str(myrandom) + "'"
        myquery = myquery + ")"

        if ((j % batchsize) != 0):
            myquery = myquery + ","

        if (((j) % batchsize) == 0):
            myquery = myquery + ";"

            try:
                cursor2.execute(str(myquery))
                #print(str(j) + " " + str(batchsize) + " " + str(j % batchsize) + myquery)
                #print(myquery)
                mybatches = mybatches + 1
            except Exception as err:
                print("ERROR: " + str(myquery) + " " + str(err))
                conn2.rollback()
            else:
                conn2.commit()
    
    try:
        cursor
    except NameError:
        pass
    else:
        cursor.close()

    try:
        conn2
    except NameError:
        pass
    else:
        tcp.putconn(conn2, close=False)

def tabledone(fn):
    global numtablesdone
    numtablesdone = numtablesdone + 1
    if fn.cancelled():
        print("{}: canceled")
    if fn.done():
        error = fn.exception()
        if error:
            print(str(fn) + ": error returned: " + str(error))
            #livethreads = livethreads - 1
        else:
            #result = fn.result()
            print(str(datetime.datetime.now()) + ": " + str(numtablesdone).zfill(6) + " Tables done. " )

    global livethreads
    livethreads = livethreads - 1
    

tcp = ThreadedConnectionPool(1, max_concurrency * 10, DSN)

pool = ThreadPoolExecutor(max_concurrency)

futures = []
for j in range(1, num_tables + 1):
    futures.append(pool.submit(write_table, j))
    mythread = futures[-1]
    mythread.add_done_callback(tabledone)
    #print(str(datetime.datetime.now()) + ": Created thread for table #" + str(j).zfill(6) + ": " + str(mythread))
print(str(datetime.datetime.now()) + " All threads created.")
#for future in concurrent.futures.as_completed(futures):
    #print(future.result())
#    pass


while True:
    mypercent = 100 * mybatches / totalbatches
    myduration = datetime.datetime.now() - starttime
    if (mybatches):
        expectedruntime = myduration / (mybatches / totalbatches)
    else:
        expectedruntime = "forever"
    print(str(datetime.datetime.now()) + ": Threads finished: " + str(str(futures).count('state=finished')) + " Running: " + str(str(futures).count('state=running')) + " Pending: " + str(str(futures).count('state=pending')) + " Cancelled: " + str(str(futures).count('state=cancelled')) + " Exceptions: " + str(str(futures).count('state=exception')) + " Completed Batches: " + str(mybatches).zfill(6) + " " + str(mypercent) + "% " + str(myduration) + " " + str(expectedruntime))
    sleep(1)
    if (str(futures).count('state=finished') == num_tables):
        break

print("Runtime of " + str(datetime.datetime.now() - starttime))
