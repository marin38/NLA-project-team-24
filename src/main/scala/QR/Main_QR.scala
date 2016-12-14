package QR

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, DenseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.QRDecomposition

object Main_QR 
{
  def main(args: Array[String]) 
  {
    val conf = new SparkConf().setAppName("Spark Template")
    val sc = new SparkContext(conf)

    //standalone
    val pathToData = "/home/anton/projects/skoltech/2/NLA/nla2016/project/data/matrix.txt"

    //val pathToData = "hdfs://10.0.0.26//user/team24/matrix.txt"

    val data = sc.textFile(pathToData)
	val matrix = new RowMatrix(data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))))

	//splitting matrix into 2 blocks
	val ncols = matrix.numCols()
	val nrows = matrix.numRows()

	val matrix_rows = matrix.rows.zipWithIndex.map((x) =>(x._2, x._1))
	val indexes = sc.parallelize(1 to nrows.toInt).map(x => if (x <= nrows/2)  1 else 0).zipWithIndex.map((x) =>(x._2, x._1))
	val matrixByBlocks = matrix_rows.join(indexes).map(x => x._2).map(x => (x._2, x._1))
	val matrixGroupedByBlocks = matrixByBlocks.groupByKey().map(x => x._2)




	//reducing 
	//val matrix_reduced = matrixGroupedByBlocks.map(x => x.map(y => (1,Vector(y))))
	val matrix_reduced = matrixGroupedByBlocks.map(x => x.map(y => (1, Matrices.dense(1, ncols.toInt, y.toArray))))
														.map(x => x.reduce((y,z) => 
	{
		if (y._1 + 1 < 2 * ncols)
			(y._1 + 1, Matrices.vertcat(Array(y._2, z._2)))
		else
			{
				val A = Matrices.vertcat(Array(y._2, z._2))

				
				/*
				BAD non serializable solution
				def matrixToRDD(m: Matrix): RDD[Vector] =
				{
				   val columns = m.toArray.grouped(m.numRows)
				   val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
				   val vectors = rows.map(row => Vectors.dense(row.toArray))
				   @transient
				   val v_p = sc.parallelize(vectors)
				   return v_p
				}*/

				//val QR = new RowMatrix(matrixToRDD(A))

				val QR = 

				(y._1 + 1, Matrices.vertcat(Array(y._2, z._2)))
			}
		}))


    matrix_reduced.collect().foreach(println)

    sc.stop()
  }
}