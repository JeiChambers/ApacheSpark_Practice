package com.jeitheronin.spark

object Parser {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(228); 
  def parseLine(line: String) ={
  	val fields = line.split(",")
  	val age = fields(2).toInt
  	val numFriends = fields(3).toInt
  	(age, numFriends) // numFriends is a tuple 2
  };System.out.println("""parseLine: (line: String)(Int, Int)""");$skip(48); 
  val lines = sc.textFile("../fakefriends.csv");System.out.println("""lines  : <error> = """ + $show(lines ));$skip(33); 
  val rdd = lines.map(parseLine);System.out.println("""rdd  : <error> = """ + $show(rdd ));$skip(122); 
  
  val totalsByAge = rdd.mapValues(x =>(x, 1)).reduceByKey((x,y)=> (x._1 + y._1, x._2 + y._2));System.out.println("""totalsByAge  : <error> = """ + $show(totalsByAge ));$skip(60);  //Key 1 is x, Key 2 is y
  val averagesByAge = totalsByAge.mapValues(x => x._1/x._2);System.out.println("""averagesByAge  : <error> = """ + $show(averagesByAge ));$skip(43); 
  
  val results = averagesByAge.collect();System.out.println("""results  : <error> = """ + $show(results ))}
  
}
