#!/usr/local/bin/python3
# pip3 install psycopg2-binary

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

DSN = "postgresql://root@35.231.25.184:26257/test" #through LB
#DSN = "postgresql://root@35.185.43.70:26257/test" #through one node

max_concurrency = 96 #one thread per table is generated
num_tables = 400 #num of tables to create
num_fields = 15 #num of fields per table
num_rows = 50000 #num of rows per table; make sure this is evently divisible by the batchsize
batchsize = 100 #number of inserts to do as a single transaction

starttime = datetime.datetime.now()
numtablesdone = 0
mybatches = 0
totalbatches = num_tables * num_rows / batchsize

#create tables
def mk_table(table_id):
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
    
    teardown(cursor, conn)

#write data to the table    
def write_table(table_id):
    global mybatches

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
                print("Retrying " + str(table_id))
                write_table(table_id)
            else:
                conn2.commit()
    teardown(cursor2, conn2)

#tear down a connect and return it to the pool
def teardown(cursor, conn):
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

def tabledone(fn):
    global numtablesdone
    numtablesdone = numtablesdone + 1
    if fn.cancelled():
        print("{}: canceled")
    if fn.done():
        error = fn.exception()
        if error:
            print(str(fn) + ": error returned: " + str(error))
        else:
            #result = fn.result()
            #print(str(datetime.datetime.now()) + ": " + str(numtablesdone).zfill(6) + " Tables done. " )
            pass

#tcp = ThreadedConnectionPool(1, max_concurrency + 8, DSN)

pool = ThreadPoolExecutor(max_concurrency)

futures = []
for j in range(1, num_tables + 1):
    futures.append(pool.submit(mk_table, j))
    mythread = futures[-1]
    mythread.add_done_callback(tabledone)
    #print(str(datetime.datetime.now()) + ": Created thread for table #" + str(j).zfill(6) + ": " + str(mythread))
print(str(datetime.datetime.now()) + " All threads queued to setup tables.")
#for future in concurrent.futures.as_completed(futures):
    #print(future.result())
#    pass

while True:
    mypercent = 100 * mybatches / totalbatches
    myduration = datetime.datetime.now() - starttime
    if (mybatches):
        expectedruntime = (myduration / (mybatches / totalbatches)) - myduration
    else:
        expectedruntime = "forever"
    print(str(datetime.datetime.now()) + ": Thr.Done:" + str(str(futures).count('state=finished')) + " WIP:" + str(str(futures).count('state=running')) + " Queue:" + str(str(futures).count('state=pending')))
    sleep(1)
    if (str(futures).count('state=finished') == num_tables):
        break


futures = []
for j in range(1, num_tables + 1):
    futures.append(pool.submit(write_table, j))
    mythread = futures[-1]
    mythread.add_done_callback(tabledone)
    #print(str(datetime.datetime.now()) + ": Created thread for table #" + str(j).zfill(6) + ": " + str(mythread))
print(str(datetime.datetime.now()) + " All threads queued to create data.")
#for future in concurrent.futures.as_completed(futures):
    #print(future.result())
#    pass


while True:
    mypercent = 100 * mybatches / totalbatches
    myduration = datetime.datetime.now() - starttime
    if (mybatches):
        expectedruntime = (myduration / (mybatches / totalbatches)) - myduration
    else:
        expectedruntime = "forever"
    print(str(datetime.datetime.now()) + ": Thr.Done:" + str(str(futures).count('state=finished')) + " WIP:" + str(str(futures).count('state=running')) + " Queue:" + str(str(futures).count('state=pending')) + " Batches done:" + str(mybatches).zfill(6) + " RT:" + str(myduration) + " Remaining:" + str(expectedruntime) + " " + str(mypercent) + "%")
    sleep(1)
    if (str(futures).count('state=finished') == num_tables):
        break

print("Runtime of " + str(datetime.datetime.now() - starttime))
