#!/usr/bin/python3
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
import logging
import argparse
import math
from psycopg2.pool import ThreadedConnectionPool

parser = argparse.ArgumentParser()
parser.add_argument("command", help="init, create, run, or cleanup; init slices up the work to be done and keeps it in a table in the db; create constructs the needed tables in the db; run does the data load; cleanup removes the config table that held the job definitions created by init")
parser.add_argument("--pgurl", help="pgurl to use to connect to the db; ex. postgres://user@server:port/db?sslmode=disable")
parser.add_argument("--threads", type=int, default=1, help="how many threads should be started? default:1")
parser.add_argument("--numtables", type=int, default=1, help="number of tables to create; default=1")
parser.add_argument("--numfields", type=int, default=2, help="number of fields to create in each table, min of 2; deafult=2")
parser.add_argument("--numrows", type=int, default=1000, help="number of rows to create in each table; default=1k")
parser.add_argument("--insertsize", type=int, default=1, help="number of inserts to do in a single query; default=1")
parser.add_argument("--batchsize", type=int, default=1000, help="number of queries to be part of each batch of work; default:1k")

args = parser.parse_args()

numtablesdone = 0
mybatches = 0
totalbatches = args.numtables * args.numrows / args.insertsize


#create tables
def mk_table(table_id, tcp, numfields):
    
    #build a table sql
    myquery = "CREATE TABLE IF NOT EXISTS table" + str(table_id).zfill(6) + " (field1 UUID PRIMARY KEY default gen_random_uuid(), field000002 INT"
    for i in range(1, numfields - 2 + 1):
        fieldnum = i + 2
        myquery = myquery + ", field" + str(fieldnum).zfill(6) + " INT"

    myquery = myquery + ");"

    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    
    teardown(cursor, conn, tcp)

#write data to the table    
def write_table(table_id, tcp, numrows, numfields, insertsize):
    global mybatches

    conn2 = tcp.getconn()
    conn2.autocommit = True
    cursor2 = conn2.cursor()
    
    for j in range(1, numrows + 1):
        if (((j-1) % insertsize) == 0):
            myquery = "INSERT INTO table" + str(table_id).zfill(6) + " (field000002"
            for i in range(1, numfields - 2 + 1):
                fieldnum = i + 2
                myquery = myquery + ",field" + str(fieldnum).zfill(6)
            myquery = myquery +  ") VALUES"
        
        myquery = myquery + "('" + str(j) + "'"
        myrandom = random.randint(1,1000)
        for i in range(1, numfields - 2  + 1):
            fieldnum = i + 2
            myquery = myquery + ", '" + str(myrandom) + "'"
        myquery = myquery + ")"

        if ((j % insertsize) != 0):
            myquery = myquery + ","

        if (((j) % insertsize) == 0):
            myquery = myquery + ";"

            try:
                cursor2.execute(str(myquery))
                mybatches = mybatches + 1
            except Exception as err:
                print("ERROR: " + str(myquery) + " " + str(err))
                conn2.rollback()
            else:
                conn2.commit()
    teardown(cursor2, conn2, tcp)

#tear down a connect and return it to the pool
def teardown(cursor, conn, tcp):
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
            pass

def createtables(pool, tcp, numtables, numfields):
    futures = []
    for j in range(1, numtables + 1):
        futures.append(pool.submit(mk_table, j, tcp, numfields))
        mythread = futures[-1]
        mythread.add_done_callback(tabledone)
    print(str(datetime.datetime.now()) + " All threads queued to setup tables.")
    #for future in concurrent.futures.as_completed(futures):
        #print(future.result())
    #    pass

    while True:
        print(str(datetime.datetime.now()) + ": Thr.Done:" + str(str(futures).count('state=finished')) + " WIP:" + str(str(futures).count('state=running')) + " Queue:" + str(str(futures).count('state=pending')))
        sleep(1)
        if (str(futures).count('state=finished') == args.numtables):
            break

def runtest(pool, tcp, starttime, numtables, numrows, numfields, insertsize):
    futures = []
    for j in range(1, numtables + 1):
        futures.append(pool.submit(write_table, j, tcp, numrows, numfields, insertsize))
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
        if (str(futures).count('state=finished') == args.numtables):
            break

def workbatches(pool, tcp, starttime):
    pass

def initcleanup(tcp):
    myquery = "DROP TABLE datagen;"
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    teardown(cursor, conn, tcp)

def inittest(pool, tcp, numtables, numrows, insertsize, batchsize):
    myquery = "CREATE TABLE IF NOT EXISTS datagen (uuid UUID PRIMARY KEY default gen_random_uuid(), sequence INT, mytable string, startval int, endval int, seed int, insertsize int, ts_start timestamp, ts_heartbeat timestamp, ts_end timestamp);"
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    
    myquery = "DELETE FROM datagen;"

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()

    #build all the work chunks in the db.
    numbatchespertbl = math.ceil(numrows/insertsize/batchsize)
  
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    seq = 1
    for t in range(1,numtables+1):
        myquery = "INSERT INTO datagen (sequence, mytable, startval, endval, seed, insertsize) VALUES "
        for i in range(1,numbatchespertbl+1):
            seed = random.randint(1,99999999)
            start = 1 + (batchsize * (i-1))
            end = i * batchsize
            myquery = myquery + "(" + str(seq) + ", 'table" + str(t).zfill(6) + "', " + str(start) + ", " + str(end) + ", " + str(seed) + ", " + str(insertsize) + ")"
            if (i < numbatchespertbl):
                myquery = myquery + ", "
            seq = seq + 1
        cursor.execute(str(myquery))
        conn.commit()
    teardown(cursor, conn, tcp)

def main():
    starttime = datetime.datetime.now()
    if (not args.pgurl):
        print("At minimum, a pgurl needs to be provided.  Please see help.")
        return 1

    #clean the inputs
    if (args.numfields < 2):
        args.numfields = 2

    if ( (args.numrows % args.insertsize) or (args.numrows % args.batchsize) ):
        print("The number of rows to be inserted needs to be an even multiple of the number of inserts to do as a single transaction and the batchsize.  Please see help.")
        return 1

    tcp = ThreadedConnectionPool(1, args.threads + 8, args.pgurl)
    pool = ThreadPoolExecutor(args.threads)

    if (args.command == "create"):
        createtables(pool, tcp, args.numtables, args.numfields)
    elif (args.command == "run"):
        runtest(pool, tcp, starttime, args.numtables, args.numrows, args.numfields, args.insertsize)
    elif (args.command == "init"):
        inittest(pool, tcp, args.numtables, args.numrows, args.insertsize, args.batchsize)
    elif (args.command == "workbatches"):
        workbatches(pool, tcp, starttime)
    elif (args.command == "cleanup"):
        initcleanup(tcp)

    print("Runtime of " + str(datetime.datetime.now() - starttime))

if __name__ == "__main__":
    main()
