# spark-hive-app
Spark hive Application

## Environment 
- Hadoop: 2.8.5
- Hive :2.3.5
- Spark: 2.4.6

## How to run the App in IntelliJ
 run the App by selecting the Class and run it.

## How to run the App in Spark submit.

````
nagaraju@nagaraju:~/spark-hive-integration-app/target$ spark-submit --master local --class com.tpb.spark.hive.ThriftHiveDBTableExample spark-hive-integration-app-1.0-SNAPSHOT.jar

```` 

## Running on Yarn 
````
/target$ spark-submit --master yarn  --class com.tpb.spark.hive.ThriftHiveDBTableExample spark-hive-integration-app-1.0-SNAPSHOT.jar
/target$ spark-submit --master yarn  --deploy-mode client --class com.tpb.spark.hive.ThriftHiveDBTableExample spark-hive-integration-app-1.0-SNAPSHOT.jar
/target$ spark-submit --master yarn  --deploy-mode cluster --class com.tpb.spark.hive.ThriftHiveDBTableExample spark-hive-integration-app-1.0-SNAPSHOT.jar


 
````

