package blocks.oneblockdecompose

import breeze.linalg.DenseMatrix

trait  Decomposition extends Serializable{
  def decompose(matrix: DenseMatrix[Double]): (DenseMatrix[Double], Array[Int])
}
