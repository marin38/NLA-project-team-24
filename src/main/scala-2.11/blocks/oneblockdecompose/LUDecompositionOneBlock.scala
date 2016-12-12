package blocks.oneblockdecompose

import breeze.linalg.DenseMatrix
import breeze.linalg.LU

class LUDecompositionOneBlock extends Decomposition {
  def decompose(matrix: DenseMatrix[Double]): (DenseMatrix[Double], Array[Int]) = {
    LU(matrix)
  }
}