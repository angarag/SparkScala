package com.mars.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object L15 {
  def main(args: Array[String]) {
    println("Welcome to the Scala worksheet")
        // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    def parseLine(line: String) {
      val fields = line.split(',')
      val age = fields(2).toInt
      val numFriends = fields(3).toInt;
      println(age,numFriends)
      (age, numFriends)
    }
    val sc = new SparkContext("local[*]", "RatingsCounter")
    val lines = sc.textFile("../fakefriends.csv")
    val rdd = lines.map(parseLine)
    for(r <- rdd)
      println(r)

  }
}