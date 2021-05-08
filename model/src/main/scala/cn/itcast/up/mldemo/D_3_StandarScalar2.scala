package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 * 标准缩放器/定标器
 *
 * @author ming
 * @date 2021/5/7 15:53
 */
object D_3_StandarScalar2 {
  def main(args: Array[String]): Unit = {
    //0.environment
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("sparkml_tfidf").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val data: DataFrame = spark.createDataFrame(
      Seq(
        (0, Vectors.dense(Array(1.0, 2.0, 3.0, 4.0, 5.0))),
        (0, Vectors.dense(Array(2.0, 3.0, 4.0, 5.0, 10.0))),
        (0, Vectors.dense(Array(3.0, 4.0, 5.0, 6.0, 14.0)))
      )
    ).toDF("label", "features")
    data.show(false)
    data.printSchema()

    //1.使用standarScaler对数据进行标准缩放
    val standardScaler: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("StandardScaler_features")
      .setWithMean(true)
      .setWithStd(true)
    val model: StandardScalerModel = standardScaler.fit(data)
    val result: DataFrame = model.transform(data)
    result.select("StandardScaler_features")show(false)

  }

}
