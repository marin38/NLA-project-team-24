package blocks.partition

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD

trait  MatrixPartition extends Serializable{
  def partitioning(matrix: RDD[IndexedRow],
                   r1: Int,
                   r2: Int,
                   n: Int,
                   batchSize: Int,
                   workersNumber: Int,
                   f1: Int,
                   f2: Int,
                   path: String): Unit
}
