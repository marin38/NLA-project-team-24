package template

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object YourClassName {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Template")
    val sc = new SparkContext(conf)

    //Place your code here
    println("Hello, world!")

    sc.stop()
  }
}