#How to use
```
$SPARK_HOME/bin/spark-submit 
    --class com.intel.oap.SparkSqlTest \
    $JAR_LOC/SparkSqlTest-1.0-SNAPSHOT.jar \
    --database-name aatpch1 \
    --test-type tpch \
    --query-filter q1,q6 \
    --round 3 \
    1>/tmp/log.txt
```

--class : class to run  

--database-name: required, test dataset  

--test-type: required, tpcds or tpch  

--query-filter: optional, queries you want to run, default will run all queries

--round: optional, how many rounds you want to run, default is 1  

--tp-test: optional, if set this parameter, will run 4 stream throughput test, and round will set to 1, each 
stream will shuffle query list randomly

--stream-num: optional, stream number for through put test, default is 4.

1>/tmp/log.txt: redirect stdout to a file, which will contains query result and execution time. you can easily 
convert result to a csv file. It's better to disable gc log for spark-driver to reduce log.
