package com.mars.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerOrders {
  
  def parseLine(x: String) = {
    val values = x.split(",")
    val cid = values(0).toInt
    val amount = values(2).toFloat
    (cid, amount)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustomerOrders")
    val lines = sc.textFile("../customer-orders.csv")
    val rdd = lines.map(parseLine)
    val result = rdd.reduceByKey((x,y)=>x+y).sortByKey()
    result.collect().foreach(println)

  }
}