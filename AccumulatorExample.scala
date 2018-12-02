/**
 * Illustrates loading a text file of integers and counting the number of invalid elements
 */

import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicLoadNums {
    def main(args: Array[String]) {
      val master = args(0)
      val inputFile = args(1)
//      val sc = new SparkContext(master, "BasicLoadNums", System.getenv("SPARK_HOME"))
      val conf = new SparkConf().setAppName("Spark Load Nums")
      val sc = new SparkContext(conf)
//      val sc = 
      val file = sc.textFile(inputFile)
      val errorLines = sc.accumulator(0)  // Create an Accumulator[Int] initialized to 0
      val dataLines = sc.accumulator(0)  // Create a second Accumulator[Int] initialized to 0
      file.foreach { line => println(line) }
      val counts = file.flatMap(line => {
        try {
          val input = line.split(" ")
          val data = Some((input(0), input(1).toInt))
          dataLines += 1
          println("dataLines => " +  dataLines)
          data
        } catch {
          case e: java.lang.NumberFormatException => {
            errorLines += 1
            println("errorLines => " +  errorLines)
            None
          }
          case e: java.lang.ArrayIndexOutOfBoundsException => {
            errorLines += 1
            println("errorLines => " +  errorLines)
            None
          }
        }
      }).reduceByKey(_ + _)
      println(counts.count)  // Printing no records in count
      if (errorLines.value < 0.1 * dataLines.value) {
        counts.saveAsTextFile("/home/cloudera/workspace2/SparkPiProject/output.txt")
      } else {
        println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
      }
    }
}
