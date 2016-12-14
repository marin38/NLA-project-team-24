package blocks.QR

package QR

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors, _}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD

object MainQR
{
  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("Spark Template")
    val sc = new SparkContext(conf)

    //standalone
    //val pathToData = "/home/anton/projects/skoltech/2/NLA/nla2016/project/data/matrix.txt"
    val pathToData = "data/matrix.txt"

    //val pathToData = "hdfs://10.0.0.26//user/team24/matrix.txt"

    val data = sc.textFile(pathToData)
    val matrix = new RowMatrix(data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))))

    val ncols = matrix.numCols()
    val nrows = matrix.numRows()
    val numPartition = 10L

    val matrixWithBlock = partitioning(matrix, numPartition = 10)
    val blockMatrix = matrixWithBlock.groupByKey().map(_._2)

    val reducedMatrix = blockProcess(blockMatrix, numPartition = numPartition, sc)

    reducedMatrix.rows.collect().foreach(println)

    sc.stop()
  }

  private def blockProcess(block: RDD[Iterable[(Long, Vector)]], numPartition: Long, sc: SparkContext): RowMatrix = {
    val matrix = block.flatMap(identity)
    val splittedMatrix = blockPartitioning(matrix, numPartition).groupByKey().map(x => processGroup(x._2, sc))
    splittedMatrix.reduce(qrTotalReduce(sc))
  }

  private def qrTotalReduce: (SparkContext => ((RowMatrix, RowMatrix) => RowMatrix)) = sc => (A1, A2) => qrReduce(A1, A2, sc)

  private def processGroup(group: Iterable[(Long, Vector)], sc: SparkContext): RowMatrix = {
    new RowMatrix(sc.parallelize(group.map(_._2).toSeq))
  }

  private def qrReduce(A1: RowMatrix, A2: RowMatrix, sc: SparkContext): RowMatrix = {
    matrixToDistributed(concatRowMatrix(A1, A2).tallSkinnyQR(false).R, sc)
  }

  private def matrixToDistributed(matrix: Matrix, sc: SparkContext): RowMatrix = {
    new RowMatrix(sc.parallelize(matrix.rowIter.toSeq))
  }

  private def concatRowMatrix(A1: RowMatrix, A2: RowMatrix): RowMatrix = {
    new RowMatrix(A1.rows.union(A2.rows))
  }

  private def partitioning(matrix: RowMatrix, numPartition: Long): RDD[(Long, (Long, Vector))] = {
    matrix.rows.zipWithIndex.map(row => (row._2.toLong / numPartition, (row._2, row._1)))
  }

  private def blockPartitioning(block: RDD[(Long, Vector)], numPartition: Long): RDD[(Long, (Long, Vector))] = {
    block.map(row => (row._1 / numPartition, (row._1, row._2)))
  }

}