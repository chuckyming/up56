package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * TODO
 * 统计学相关API
 *
 * @author ming
 * @date 2021/5/7 14:04
 */
object B_StaticDemo {
  def main(args: Array[String]): Unit = {
    //0.evironment
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("model").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val rdd: RDD[Array[Double]] = sc.parallelize(List(Array(1.0), Array(2.0), Array(3.0), Array(4.0), Array(5.0)))
    val vectorRDD: RDD[linalg.Vector] = rdd.map(arr => Vectors.dense(arr))

    println("1.查看统计学特征")
    //多元统计摘要
    val summary: MultivariateStatisticalSummary = Statistics.colStats(vectorRDD)
    println(summary.max)
    println(summary.min)
    println(summary.mean)
    println(summary.variance)
    println(summary.normL1)
    println(summary.normL2)

    println("2.查看统计学相关性")
    val x: RDD[Double] = sc.parallelize(Array(1,2,3,4,5))
    val y: RDD[Double] = sc.parallelize(Array(10,20,30,40,50))
    val z: RDD[Double] = sc.parallelize(Array(-10,-20,-30,-40,-50))
    val corr1: Double = Statistics.corr(x, y) //1.0, 相关系数
    val corr2: Double = Statistics.corr(x, z)
    val corr3: Double = Statistics.corr(x, y, "spearman")
    println(corr1, corr2, corr3)

  }
}
