/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tpb.spark.hive

// $example on:spark_hive$
import org.apache.spark.sql.SparkSession
// $example off:spark_hive$

object HiveTableVsSparkTable {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Thrift Hive Example")
      .master("local")
      .config("hive.metastore.uris","thrift://localhost:9083")// When I keep this , then it is working fine.
      // Note: Hive server should be running.
      .config("spark.sql.warehouse.dir", "/user/nagaraju/warehouse")
      // When i keep only that above then I am not able to connect hdfs location.
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"naga_$i")))
    recordsDF.createOrReplaceTempView("src")


    // Queries are expressed in HiveQL
    println("Showing the data from Hive 5 ")
    sql("SELECT * FROM src").show(5)

    println("Showing the table data from local 10 ")
    sql("select * from src").show(10)

    // Note: when we create same table name in Hive and Local then it will read the data from Local only

    spark.stop()
    // $example off:spark_hive$
  }
}
