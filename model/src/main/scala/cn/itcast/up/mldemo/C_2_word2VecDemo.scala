package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 *
 * @author ming
 * @date 2021/5/7 15:02
 */
object C_2_word2VecDemo {
  //0.evironment
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkMlilb")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val data: DataFrame = spark.createDataFrame(
      Seq(
        "Hi I heard about Spark".split(" "),
        "I wish Java could user case classes".split(" "),
        "Logistic regeression models are neat".split(" ")
      ).map(Tuple1.apply)
    ).toDF("text")
    data.show(false)

    //2.使用word2vec
    val word2Vec: Word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model: Word2VecModel = word2Vec.fit(data)
    val result: DataFrame = model.transform(data)
    result.show(false)
  }
}
