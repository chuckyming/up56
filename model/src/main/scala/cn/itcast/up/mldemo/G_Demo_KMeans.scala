package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 * KMeans
 *
 * @author ming
 * @date 2021/5/8 9:53
 */
object G_Demo_KMeans {
  def main(args: Array[String]): Unit = {
    //0.evironment
    val spark: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val irisDF: DataFrame = spark.read.format("libsvm").load("file:///G:\\data\\up\\ml\\iris_KMeans.libsvm")
    irisDF.show(false)
    irisDF.printSchema()

    //1.特征工程-数据归一化/标准化
    val scalerDF: DataFrame = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaler_features")
      .fit(irisDF)
      .transform(irisDF)
    scalerDF.show(false)

    //2.训练KMeans算法模型
    val model:KMeansModel = new KMeans()
      .setK(3) //设置k值，也就是聚为积累
      .setSeed(100) //设置随机种子，保证每次执行使用的随机种子一样
      .setMaxIter(20) //设置最大迭代次数
      .setFeaturesCol("scaler_features")//设置特征是哪个，应该使用归一化之后的
      .setPredictionCol("predictClusterIndex")//设置预测列名称，注意：预测出来的是据类中心的索引编号
      .fit(scalerDF) //填充/适合/训练

    //3.使用模型进行预测
    val result: DataFrame = model.transform(scalerDF)

    //4.查看聚类效果
    result.show(false)
    result.groupBy("label", "predictClusterIndex").count().show(false)



  }
}
