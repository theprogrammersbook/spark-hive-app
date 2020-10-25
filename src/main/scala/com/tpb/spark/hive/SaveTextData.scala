package com.tpb.spark.hive

import org.apache.spark.sql.SparkSession

object SaveTextData extends App {
   // Creating spark session
  val spark = SparkSession.builder()
    .appName("SaveTextFile")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

}
