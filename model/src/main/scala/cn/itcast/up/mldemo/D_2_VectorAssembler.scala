package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 * 多向量合并为一个向量
 *
 * @author ming
 * @date 2021/5/7 15:42
 */
object D_2_VectorAssembler {
  def main(args: Array[String]): Unit = {
    //0.environment
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sparkml").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val data: DataFrame = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    data.show(false)

    //1.创建VectorAssembler向量装配器
    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures", "clicked"))
      .setOutputCol("vectorAssember_feature")
    val reau: DataFrame = vectorAssembler.transform(data)

    reau.show(false)


  }

}
