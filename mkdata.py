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
import socket
import os
import argparse
import math
from psycopg2.pool import ThreadedConnectionPool

parser = argparse.ArgumentParser()
parser.add_argument("command", help="init, run, watch, or cleanup; init slices up the work to be done and keeps it in a table in the db and creates the destination tables; run does the data load and you can have multiple processes running at once; watch allows you to monitor the state of the batch queue; cleanup removes the config table that held the job definitions created by init")
parser.add_argument("--pgurl", help="pgurl to use to connect to the db; ex. postgres://user@server:port/db?sslmode=disable")
parser.add_argument("--threads", type=int, default=1, help="how many threads should be started? default:1")
parser.add_argument("--numtables", type=int, default=1, help="number of tables to create; default=1")
parser.add_argument("--numfields", type=int, default=2, help="number of fields to create in each table, min of 2; deafult=2")
parser.add_argument("--numrows", type=int, default=1000, help="number of rows to create in each table; default=1k")
parser.add_argument("--insertsize", type=int, default=100, help="number of inserts to do in a single query; default=100")
parser.add_argument("--batchsize", type=int, default=1000, help="number of queries to be part of each batch of work; default:1k")

args = parser.parse_args()

numtablesdone = 0
mybatches = 0
totalbatches = args.numtables * args.numrows / args.insertsize


#create tables
def mk_table(table_id, tcp, numfields):
    
    #build a table sql
    myquery = "CREATE TABLE IF NOT EXISTS table" + str(table_id).zfill(6) + " (field000001 UUID PRIMARY KEY default gen_random_uuid(), field000002 INT"
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
    
    for j in range(1, numrows +1):
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

def write_batch(tcp, mytable, startval, endval, numfields, seed, insertsize, batchid):
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    mystartval = startval
    myendval = startval + insertsize
    loops = round((endval-startval)/insertsize)
    mylastheartbeat = datetime.datetime.now()
    i = 1 # number of statements
    while (i <= loops):
        myquery = "UPSERT INTO " + str(mytable) + " (field000002"
        for j in range(3, numfields+1):
            myquery = myquery + ", field" + str(j).zfill(6)
        myquery = myquery +  ") VALUES"

        myquery = myquery + "('" + str(mystartval) + "'"
        for j in range(3, numfields+1):
            myquery = myquery + ", '" + str(seed) + "'"
        myquery = myquery + ")"
        for k in range(mystartval+1, myendval):
            myquery = myquery + ", ('" + str(k) + "'"
            for j in range(3, numfields+1):
                myquery = myquery + ", '" + str(seed) + "'"
            myquery = myquery + ")"
        myquery = myquery + ";"
        cursor.execute(str(myquery))
        mystartval = myendval + 1
        myendval = myendval + insertsize
        if ( datetime.datetime.now() > mylastheartbeat + datetime.timedelta(seconds=10) ):
            mylastheartbeat = datetime.datetime.now()
            myquery = "UPDATE datagen set ts_heartbeat = '" + str(mylastheartbeat) + "' WHERE id = '" + str(batchid) + "' limit 1;"
            cursor.execute(str(myquery))
        i += 1

    ts = datetime.datetime.now()
    myquery = "UPDATE datagen SET ts_end = '" + str(ts) + "' WHERE id = '" + str(batchid) + "' limit 1;"
    cursor.execute(str(myquery))
    teardown(cursor, conn, tcp)

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

def workbatches(pool, tcp, starttime, maxthreads):
    futures = []
    mymaxthreadcount = maxthreads

    worktobedone = 1
    while worktobedone:
        if str(futures).count('state=running') < mymaxthreadcount:
            #get the next batch
            conn = tcp.getconn()
            conn.autocommit = True
            cursor = conn.cursor()
            hostname = socket.gethostname()
            pid = os.getpid()
            ts = datetime.datetime.now()
            myquery = "update datagen set workerid = '" + str(hostname) + ":" + str(pid) + "', ts_start = '" + str(ts) + "' WHERE ts_start is null limit 1 returning id, mytable, startval, endval, numfields, seed, insertsize;"
            cursor.execute(str(myquery))
            row = cursor.fetchone()
            teardown(cursor,conn,tcp)
            if row is None:
                break
            # we have now checked out a batch of work, the tuple 'row' now has the important details in it to do work
            futures.append(pool.submit(write_batch, tcp, row[1], row[2], row[3], row[4], row[5], row[6], row[0]))
            mythread = futures[-1]
            mythread.add_done_callback(tabledone)

def dropitall(tcp):
    myquery = "USE defaultdb; DROP database test cascade; create database test;"
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

def inittest(pool, tcp, numtables, numrows, insertsize, batchsize, numfields):
    myquery = "CREATE TABLE IF NOT EXISTS datagen (id UUID PRIMARY KEY default gen_random_uuid(), sequence INT, mytable string, startval int, endval int, numfields int, seed int, insertsize int, workerid string, ts_start timestamp, ts_heartbeat timestamp, ts_end timestamp);"
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
    numbatchespertbl = math.ceil(numrows/batchsize)
  
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    seq = 1
    for t in range(1,numtables+1):
        myquery = "INSERT INTO datagen (sequence, mytable, startval, endval, seed, insertsize, numfields) VALUES "
        for i in range(1,numbatchespertbl+1):
            seed = random.randint(1,99999999)
            start = 1 + (batchsize * (i-1))
            end = i * batchsize
            myquery = myquery + "(" + str(seq) + ", 'table" + str(t).zfill(6) + "', " + str(start) + ", " + str(end) + ", " + str(seed) + ", " + str(insertsize) + ", " + str(numfields) + ")"
            if (i < numbatchespertbl):
                myquery = myquery + ", "
            seq = seq + 1
        cursor.execute(str(myquery))
        conn.commit()
    teardown(cursor, conn, tcp)
    createtables(pool, tcp, numtables, numfields)

def watch(tcp):
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    mylastcleanup = datetime.datetime.now()
    while True:
        myquery = "SELECT count(*) from datagen;"
        cursor.execute(str(myquery))
        totalbatches = cursor.fetchone()[0]
        myquery = "SELECT count(*) from datagen where ts_end is not null;"
        cursor.execute(str(myquery))
        done = cursor.fetchone()[0]
        myquery = "SELECT count(*) from datagen where ts_end is null and ts_start is not null;"
        cursor.execute(str(myquery))
        wip = cursor.fetchone()[0]
        remaining = totalbatches - wip - done
        print(str(datetime.datetime.now()) + " Batches: " + str(totalbatches) + "; Done: " + str(done) + "; WIP: " + str(wip) + "; Remaining: " + str(remaining))
        if (done == totalbatches):
            break
        if ( datetime.datetime.now() > mylastcleanup + datetime.timedelta(seconds=60) ):
            mylastcleanup = datetime.datetime.now()
            oldts = datetime.datetime.now() - datetime.timedelta(seconds=120)
            myquery = "UPDATE datagen set ts_heartbeat = NULL, ts_start = NULL WHERE ts_end is null and ( ts_heartbeat < '" + str(oldts) + "' OR ts_heartbeat is null ) ;"
            result = cursor.fetchone()
            print("I tried to reset some stale batches back to new so they could be worked. " + str(result))
            cursor.execute(str(myquery))
        sleep(2)

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
    pool = ThreadPoolExecutor(args.threads+4)

    if (args.command == "create"):
        createtables(pool, tcp, args.numtables, args.numfields)
    elif (args.command == "oldrun"):
        runtest(pool, tcp, starttime, args.numtables, args.numrows, args.numfields, args.insertsize)
    elif (args.command == "init"):
        inittest(pool, tcp, args.numtables, args.numrows, args.insertsize, args.batchsize, args.numfields)
    elif (args.command == "run"):
        workbatches(pool, tcp, starttime, args.threads)
    elif (args.command == "cleanup"):
        initcleanup(tcp)
    elif (args.command == "drop"):
        dropitall(tcp)
    elif (args.command == "watch"):
        watch(tcp)

    pool.shutdown(wait=True)
    print("Runtime of " + str(datetime.datetime.now() - starttime))

if __name__ == "__main__":
    main()
