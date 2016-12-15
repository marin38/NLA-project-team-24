import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import decomposition.QR

import scala.reflect.ClassTag

object speedTest {
  def main(args: Array[String]) {
    val i: Int = args(0).toInt
    val master = args(1)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val conf = new SparkConf().setAppName("Spark Template").setMaster("local[4]")
    val conf = new SparkConf().setAppName("Spark Template").setMaster("spark://" + master + ":7077")
    val sc = new SparkContext(conf)

    //for (i <- 1 to 10)ф
    time(new QR().decompose("hdfs://" + master + ":9000/" + "matrix_m_" + i * 1000, (i * 100f / 8 * 10).toLong, (i * 100f / 8 * 10).toLong / 10, sc).rows.persist())
    sc.stop()
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / (1000 * 1000 * 1000) + "s")
    result
  }

}