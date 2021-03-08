# datasimulator
A data creation and simulation tool for CockroachDB

ToDo:
* clean everything up 
* turn stuff into functions/methods
* separate table creation into separate threads - done
* use both multiprocess and multithread approaches - 400 threads doesn't scale well
* have the --init build a queue of work
* have the --run spawn multiple processes with multiple threads each
* adjust load from read amplification - SELECT max(metrics->'rocksdb.read-amplification') FROM crdb_internal.kv_store_status
* adjust load by transaction duration - select * from crdb_internal.node_metrics where name = 'sql.txn.latency-p99
* get rid of connection pooling

My notes for updating VMs:
```
sudo apt update -y
sudo apt upgrade -y
sudo apt install python3-pip -y
pip3 install psycopg2-binary
````
