package com.jeitheronin.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(0).toString
    val numFriends = fields(3).toInt
    (name, numFriends)
  }
  
  // Main Function
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine    
    val sc = new SparkContext("Local[*]", "FriendsByName")
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")
    
    // Use our parseLines function to convert to ()
  }
}