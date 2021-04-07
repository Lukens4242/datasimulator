# datasimulator
A data creation and simulation tool for CockroachDB

ToDo:
* clean everything up 
* turn all queries into parameterized execution
* turn stuff into functions/methods
* DONE: separate table creation into separate threads
* DONE: use both multiprocess and multithread approaches - 400 threads doesn't scale well
* DONE: have the --init build a queue of work
* adjust load from read amplification - SELECT max(metrics->'rocksdb.read-amplification') FROM crdb_internal.kv_store_status
* adjust load by transaction duration - select * from crdb_internal.node_metrics where name = 'sql.txn.latency-p99
* DONE: use upserts for when restarts are needed
* DONE: add a heartbeat ts for each batch

My notes for updating VMs:
```
sudo apt update -y
sudo apt upgrade -y
sudo apt install python3-pip -y
pip3 install psycopg2-binary
````
