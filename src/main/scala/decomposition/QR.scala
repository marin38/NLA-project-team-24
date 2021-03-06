package decomposition

import breeze.optimize.BatchSize
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.reflect.ClassTag

class QR extends Serializable {
  def decompose(pathToData: String, batchSize: Long, smallBatchSize: Long, sc: SparkContext): RowMatrix = {
    val data = sc.textFile(pathToData)
    val matrix = new RowMatrix(data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))))

    val ncols = matrix.numCols()
    val nrows = matrix.numRows()

    //get rdd with big block numbers
    val matrixWithBlock = partitioning(matrix, batchSize = batchSize)

    //convert rdd to Iterable[RDD], each element of this Iterable is big block
    val blockMatrix = groupByKeyToRDDs(matrixWithBlock).values

    //split each big block into small blocks and reduce it with spark's QR
    val reducedMatrix = blockMatrix.map(block => blockProcess(block, smallBatchSize, sc))

    //reduce reduced matrix
    val secondaryReducedMatrix = reducedMatrix.reduce(qrTotalReduce(sc))

    //secondaryReducedMatrix.rows.collect.foreach(println)
    secondaryReducedMatrix
  }

  private def blockProcess(block: RDD[(Long, Vector)], numPartition: Long, sc: SparkContext): RowMatrix = {
    //val splittedMatrix = blockPartitioning(block, numPartition).groupByKey().map(x => processGroup(x._2, sc))
    val splittedMatrix = groupByKeyToRDDs(blockPartitioning(block, numPartition)).values.map(x => x.map(y => y._2))
      .map(x => new RowMatrix(x))
    splittedMatrix.reduce(qrTotalReduce(sc))
  }

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

  private def partitioning(matrix: RowMatrix, batchSize: Long): RDD[(Long, (Long, Vector))] = {
    /*
    This function for splitting input matrix into big blocks.
    First Long of output is big block number
     */
    matrix.rows.zipWithIndex.map(row => (row._2.toLong / batchSize, (row._2, row._1)))
  }

  //split each big block into small blocks
  private def blockPartitioning(block: RDD[(Long, Vector)], batchSize: Long): RDD[(Long, (Long, Vector))] = {
    block.map(_._2).zipWithIndex.map(row => (row._2 / batchSize, (row._2, row._1)))
  }

}