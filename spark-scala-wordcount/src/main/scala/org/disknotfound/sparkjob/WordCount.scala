package org.disknotfound.sparkjob

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

object WordCount {
  val JOB_NAME = "WordCount"
  val TIMESTAMP = "MM-dd-yyyy_HHmmss"
  val INPUT_FILE_LOCATION = "file:///E:/tmp/"
  val OUTPUT_FILE_PREFIX = "-count-"

  def main(args: Array[String]) = {
    val df: DateFormat = new SimpleDateFormat(TIMESTAMP)
    val today: Date = Calendar.getInstance.getTime

    // Start the Spark context
    val conf = new SparkConf()
      .setAppName(JOB_NAME)

    val sc = new SparkContext(conf)

    // Read some example file to a test RDD
    val test = sc.textFile(INPUT_FILE_LOCATION + args(0))

    test.flatMap { line => // for each line
      line.split(" ") // split the line in word by word
    }.map { word => // for each word
      (word, 1) // return key/value tuple, with the word as key and 1 as value
    }.reduceByKey(_ + _) // Sum all of the value with same key
      .saveAsTextFile(INPUT_FILE_LOCATION + args(0) + OUTPUT_FILE_PREFIX + df.format(today)) // Save to a text file

    // Stop the Spark context
    sc.stop

  }
}