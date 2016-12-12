package blocks.partition

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class LUBlockPartitioning(private val rootPath: String) extends MatrixPartition{
  def partitioning(matrix: RDD[IndexedRow],
                   r1: Int,
                   r2: Int,
                   n: Int,
                   batchSize: Int,
                   workersNumber: Int,
                   f1: Int,
                   f2: Int,
                   path: String = ""): Unit = {
    //val rootPath = "hdfs://" + HDFSMaster + ":9000/"
    if (n < batchSize)
      {
        val A1 = matrix.filter(x => (x.index >= r1) && (x.index <= r2))
        saveBlock(A1, rootPath, 1, r1, n, batchSize, -1)
      }

    else {
      if (r1 < n.toFloat / 2) {
        partitioning(matrix, r1, r2, n / 2, batchSize, workersNumber / 2, f1, f2, path + "A1/")
        for(i <- 0 until  batchSize) saveBlock(slicer(matrix, r1, r2, n / 2, n), path, 2, r1, n, batchSize, i)
      }

      else {
        for(i <- 0 until (r2 - r1) * batchSize / 2 * n) saveBlock(slicer(matrix, r1, r1 + 2 * batchSize * i / n, 0, n / 2), path, 3, r1, n, batchSize, i)

        for {j <- 0 until f2
             i <- 0 until (2 * (r2 - r1) * f1 / n)
        } yield saveBlock(slicer(matrix, r1, r1 + (i + 1) * n / 2 * f1, n / 2, n / 2 + (j + 1) * n/ 2 * f2), path, 3, r1, n, batchSize, i, (2 * f1 * r1 / n - f1) * f2 + j)

      }
    }
  }

  private def saveBlock(matrix: RDD[IndexedRow], rootPath: String, blockNumber: Int, r1: Int, n: Int, batchSize: Int,
                        i: Int, el: Int = -1) = {
    val fileName = blockNumber match {
      case 1 => rootPath + "A" + blockNumber + "/A_" + r1 * batchSize / n + ".txt"
      case 2 => rootPath + "A" + blockNumber + "/A_" + i + "_" + r1 * batchSize / n + ".txt"
      case 3 => rootPath + "A" + blockNumber + "/A_" + (2 * r1 - n)* batchSize / 4 * n + i + ".txt"
      case _ => rootPath + "A" + blockNumber + "/A_" + el + "_" + i + ".txt"

    }
    matrix.saveAsTextFile(fileName)
  }

  private def slicer(matrix: RDD[IndexedRow], row1: Int, row2: Int, col1: Int, col2: Int): RDD[IndexedRow] ={
    matrix.filter(x => (x.index >= row1) && (x.index <= row2)).map(x => IndexedRow(x.index, Vectors.dense(x.vector.toArray.slice(col1, col2))))
  }
}
