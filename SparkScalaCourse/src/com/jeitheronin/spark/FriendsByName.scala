package com.jeitheronin.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(2).toString
    val numFriends = fields(3).toInt
    (name, numFriends)
  }
}