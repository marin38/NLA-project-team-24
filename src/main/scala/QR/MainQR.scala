package QR

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level


import scala.reflect.ClassTag

object MainQR {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Spark Template").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //standalone
    //val pathToData = "/home/anton/projects/skoltech/2/NLA/nla2016/project/data/matrix.txt"

    val pathToData = "data/matrix.txt"
    //val pathToData = "hdfs://10.0.0.26//user/team24/matrix.txt"

    val data = sc.textFile(pathToData)
    val matrix = new RowMatrix(data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))))

    val ncols = matrix.numCols()
    val nrows = matrix.numRows()

    //size of big block
    //workersNumber = rowNumber / batchsize
    val batchSize = 15L
    //number of small blocks in each big block
    val numPartition = 10L

    //get rdd with big block numbers
    val matrixWithBlock = partitioning(matrix, numPartition = batchSize)
    println("lol")
    matrixWithBlock.collect.foreach(println)
    //val blockMatrix = matrixWithBlock.groupByKey().map(_._2)

    //convert rdd to Iterable[RDD], each element of this Iterable is big block
    val blockMatrix = groupByKeyToRDDs(matrixWithBlock).values

    println("block matrix")
    blockMatrix.foreach(x => x.collect.foreach(println))

    //val reducedMatrix = blockProcess(blockMatrix, numPartition = numPartition, sc)

    //split each big block into small blocks and reduce it with spark's QR
    val reducedMatrix = blockMatrix.map(block => blockProcess(block, numPartition, sc))

    //reducedMatrix.rows.collect().foreach(println)
    reducedMatrix.foreach(x => x.rows.collect.foreach(println))

    sc.stop()
  }

  private def blockProcess(block: RDD[(Long, Vector)], numPartition: Long, sc: SparkContext): RowMatrix = {
    //val splittedMatrix = blockPartitioning(block, numPartition).groupByKey().map(x => processGroup(x._2, sc))
    val splittedMatrix = groupByKeyToRDDs(blockPartitioning(block, numPartition)).values.map(x => x.map(y => y._2)).map(x => new RowMatrix(x))
    splittedMatrix.reduce(qrTotalReduce(sc))
  }

  /*
  private def blockProcess(block: RDD[Iterable[(Long, Vector)]], numPartition: Long, sc: SparkContext): RowMatrix = {
    val matrix = block.flatMap(identity)
    val splittedMatrix = blockPartitioning(matrix, numPartition).groupByKey().map(x => processGroup(x._2, sc))
    splittedMatrix.reduce(qrTotalReduce(sc))
  }
  */

  //scala magic for converting RDD to Iterable[RDD]
  private def groupByKeyToRDDs[K, V](pairRDD: RDD[(K, V)]) (implicit kt: ClassTag[K],
                                                    vt: ClassTag[V], ord: Ordering[K]): Map[K, RDD[V]] = {
    val keys = pairRDD.keys.distinct.collect
    (for (k <- keys) yield
      k -> pairRDD.filter(_._1 == k).values
      ).toMap
  }

  //Spark can't serialize spark context, but it required for converted Matrix to RowMatrix
  private def qrTotalReduce: (SparkContext => ((RowMatrix, RowMatrix) => RowMatrix)) = sc => (A1, A2) => qrReduce(A1, A2, sc)

  //out of date
  private def processGroup(group: Iterable[(Long, Vector)], sc: SparkContext): RowMatrix = {
    new RowMatrix(sc.parallelize(group.map(_._2).toSeq))
  }

  private def qrReduce(A1: RowMatrix, A2: RowMatrix, sc: SparkContext): RowMatrix = {
    /*
     Spark's QR return R as not distributed Matrix, we should transform it to distributed for reduce
     */
    matrixToDistributed(concatRowMatrix(A1, A2).tallSkinnyQR(false).R, sc)
  }

  private def matrixToDistributed(matrix: Matrix, sc: SparkContext): RowMatrix = {
    new RowMatrix(sc.parallelize(matrix.rowIter.toSeq))
  }

  private def concatRowMatrix(A1: RowMatrix, A2: RowMatrix): RowMatrix = {
    new RowMatrix(A1.rows.union(A2.rows))
  }

  private def partitioning(matrix: RowMatrix, numPartition: Long): RDD[(Long, (Long, Vector))] = {
    /*
    This function for splitting input matrix into big blocks.
    First Long of output is big block number
     */
    matrix.rows.zipWithIndex.map(row => (row._2.toLong / numPartition, (row._2, row._1)))
  }

  //out of date
  private def blockPartitioning(block: RDD[(Long, Vector)], batchSize: Long): RDD[(Long, (Long, Vector))] = {
    block.map(row => (row._1 / batchSize, (row._1, row._2)))
  }

}