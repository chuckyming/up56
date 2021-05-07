package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 *
 * @author ming
 * @date 2021/5/7 15:20
 */
object D_1_StringIndexer_IndexToString_OneHotEncoder {
  def main(args: Array[String]): Unit = {
    //0.evironment
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("sparkml_tfidf").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val data: DataFrame = spark.createDataFrame(
      Seq((0, "红"), (1, "绿"), (2, "蓝"))
    ).toDF("id", "words")
    data.show()

    //sparkmllib中后续的计算都是基于向量/矩阵
    //但是实际中的数据的特诊很多是字符串，所以需要对字符串进行编码
    //1.StringIndexer将字符串编码位数字，
    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("words")
      .setOutputCol("index_words")
    val model: StringIndexerModel = stringIndexer.fit(data)
    val indexResult: DataFrame = model.transform(data)
    indexResult.show(false)

    //2.indextostring将数字解码为字符串，如1234567解码为红橙黄绿
    val indexToString: IndexToString = new IndexToString()
      .setInputCol("index_words")
      .setOutputCol("before_words")
      .setLabels(model.labels)
    val beforeResult: DataFrame = indexToString.transform(indexResult)
    beforeResult.show(false)

    //3.onehotencoder
    //onehotencoder的input必须为index数字类型，所以需要先进性StringIndexer
    val oneHotEncoder: OneHotEncoder = new OneHotEncoder()
      .setInputCol("index_words")
      .setOutputCol("oneHot_Result")
    val oneHotResult: DataFrame = oneHotEncoder.transform(indexResult)
    oneHotResult.show(false)
  }

}
