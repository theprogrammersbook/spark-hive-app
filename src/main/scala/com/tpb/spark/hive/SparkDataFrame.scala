package com.tpb.spark.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case class Record(name:String, character:String, newVal:Array[String])

object SparkDataFrame extends App{
  val spark =      SparkSession.builder
    .appName("SparkDataFrame")
    .master("local")
    .getOrCreate()


  import spark.implicits._

  val data = Seq(("Nagaraju","Good:Bad")).toDF("Name","Character")
  val df2 =data.withColumn("newVal",split($"Character",":"))
  //Using explode
  val df3 = df2.select(explode('newVal))
    .withColumn("col1",when('col==="Good","Good") as("One"))
    .withColumn("col2",when('col==="Bad","Bad") as("Two"))
  df3.show()

  /*
  +----+----+----+
| col|col1|col2|
+----+----+----+
|Good|Good|null|
| Bad|null| Bad|
+----+----+----+

   */

 // df2.select('Name,'Character,explode('newVal)).show() // explode is working like this
/*
+--------+---------+----+
|    Name|Character| col|
+--------+---------+----+
|Nagaraju| Good:Bad|Good|
|Nagaraju| Good:Bad| Bad|
+--------+---------+----+

 */


  /*
  //Converting the array to individuals
  val encoder = org.apache.spark.sql.Encoders.product[Record]
 val ds1 = df2.as(encoder)
   val ds2 = ds1.map(r => (r.name,r.character,r.newVal(0),r.newVal(1))).toDF()
  ds2.show()
  */

  /*
  +--------+--------+----+---+
|      _1|      _2|  _3| _4|
+--------+--------+----+---+
|Nagaraju|Good:Bad|Good|Bad|
+--------+--------+----+---+

   */

/*
  // Get each value from array
 val df3 =df2.withColumn("first",$"new".getItem(0))
  .withColumn("second",$"new".getItem(1))
  df3.show
  */

  /*
  +--------+---------+-----------+-----+------+
|    Name|Character|        new|first|second|
+--------+---------+-----------+-----+------+
|Nagaraju| Good:Bad|[Good, Bad]| Good|   Bad|
+--------+---------+-----------+-----+------+

   */
}
