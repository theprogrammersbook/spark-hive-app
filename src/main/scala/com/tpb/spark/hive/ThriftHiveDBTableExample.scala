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

import org.apache.spark.sql.SparkSession

object ThriftHiveDBTableExample {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("ThriftHiveDBTableExample")
      .master("local")
      .config("hive.metastore.uris","thrift://localhost:9083")// When I keep this , then it is working fine.
      // Note: Hive server should be running.
      .config("spark.sql.warehouse.dir", "/user/nagaraju/warehouse")
      // When i keep only that above then I am not able to connect hdfs location.
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("show databases").show()

  //  sql("CREATE DATABASE IF NOT EXISTS hive")  // Giving error not able to create db.

    sql("CREATE TABLE IF NOT EXISTS hive.src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    spark.stop()
  }
}
