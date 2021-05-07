package cn.itcast.up.mldemo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Matrices, Matrix}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * TODO
 * localVector本地向量
 * LabelPoint标签向量
 * LocalMatrix本地矩阵
 * DistributeMatrix分布式矩阵
 *
 * @author ming
 * @date 2021/5/7 8:51
 */
object A_DataTypeDemo {
  def main(args: Array[String]): Unit = {
    println("=========LocalVector本地向量")
    //todo loclavector本地向量
    //本地向量分为：
    //密集向量：（9，5，0，2，7）
    //系数向量（不为0的个数4，不为0的位置（0，1，3，4），不为0的数据（9，5，2，7））
    val vector1: linalg.Vector = Vectors.dense(9,5,0,2,7)
    val vector2: linalg.Vector = Vectors.sparse(4, Array(0, 1, 3,4), Array(9,5,2,7))

    println("-----todo labepoint标签向量")
    val point1: LabeledPoint = LabeledPoint(0, vector1)
    val point2: LabeledPoint = LabeledPoint(0, vector2)

    println("------本地矩阵")
    val matrix1: Matrix = Matrices.dense(3,2,Array(1,2,0,4,5,6))

    val matrix2: Matrix = Matrices.sparse(3,2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6,8))

    println("------------todo distributedmatrix分布式矩阵")
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ml")
    val sc: SparkContext = new SparkContext(sparkconf)
    sc.setLogLevel("WARN")
    val rdd: RDD[Array[Double]] = sc.parallelize(List(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0)))
    val vectroRDD: RDD[linalg.Vector] = rdd.map(arr => Vectors.dense(arr))



    println("----rowmatrix矩阵")
    //rowmatrix矩阵
    val rowmatrix: RowMatrix = new RowMatrix(vectroRDD)
    rowmatrix.rows.foreach(println)

    println("-------indexedrowmatrix行索引矩阵")
    val indexedrowRDD: RDD[IndexedRow] = vectroRDD.map(v => {
      new IndexedRow(v.size, v)
    })
    val indexedRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(indexedrowRDD)
    indexedRowMatrix.rows.foreach(println)

  }

}
