package template

import org.apache.spark.sql.SparkSession

object YourClassName 
{
  def main(args: Array[String]) 
  {
    val spark = SparkSession
      .builder
      .appName("Spark Template: hello world")
      .getOrCreate()

    println("Hello, world!")

    spark.stop()
  }
}
