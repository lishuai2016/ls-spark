package com.spark.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object WordCount {

  def main(args: Array[String]): Unit = {
    //println("hello world")
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.7.3")
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //    val lines = sc.textFile("hdfs://spark001:9000/spark.txt", 1)
    val lines = sc.textFile("spark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordcounts = pairs.reduceByKey { _ + _ }
//    val sortedwordcounts = wordcounts.map(x=>((-1)*x._2,x._1)).sortByKey().map(x=>(x._2,(-1)*x._1))
    val sortedwordcounts = wordcounts.sortBy(x=>x._2, false)
    sortedwordcounts.foreach { x => println(x._1 + " appears " + x._2 + " times.") }
  }
}