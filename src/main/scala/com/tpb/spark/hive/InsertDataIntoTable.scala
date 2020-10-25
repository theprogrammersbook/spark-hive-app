package com.tpb.spark.hive

import org.apache.spark.sql.SparkSession

object InsertDataIntoTable extends App {
 // Note ,we can not create Hive database in Spark
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir", "/user/nagaraju/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  // Import spark sql
  println("Started ")
  import spark.sql
 sql("use default")
  sql("create table if not exists emp_spark (id int, name string)")
  sql("insert into emp_spark values(1,'Nagaraju')")
  println("End'")
}
