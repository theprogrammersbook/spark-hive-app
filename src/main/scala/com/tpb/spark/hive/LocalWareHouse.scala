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
import java.io.File

import org.apache.spark.sql.SparkSession
// $example off:spark_hive$

object LocalWareHouse {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {
    // Not copied hive-site.xml to spark location.
    // So, Spark will create its own metastore and own warehouse with the below specified locaiton
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    /*
      Problem:
      The root scratch dir: /tmp/hive on HDFS should be writable. Current permissions are: rw-rw-rw- (on Windows)
      Solution:
      Local:
      nagaraju@nagaraju:/tmp$ sudo chmod 777 -R hive/  OR
      HFDS:
      nagaraju@nagaraju:~$ hdfs dfs -chmod -R  777  /tmp
      nagaraju@nagaraju:~$ hdfs dfs -ls /

     */

    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
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
    // $example off:spark_hive$
  }
}
