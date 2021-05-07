package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TODO
 * 演示特征工程——特征提取-TFIDF
 *
 * @author ming
 * @date 2021/5/7 14:33
 */
object C_1_TFIDFDemo {
  def main(args: Array[String]): Unit = {
    //0.evironment
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("spakml_tfidf").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val dat: DataFrame = spark.createDataFrame(
      Seq(
        (0, "Hi I heard about Spark Spark Spark Spark Spark"),
        (0, "I wish Java could use case classes"),
        (1, "Logistic regression models are neat")
      )
    ).toDF("label", "words")

    //1.分词
    val token: Tokenizer = new Tokenizer()
      .setInputCol("words")//指定对那一列进行分词
      .setOutputCol("token_words")//指定分完之后的列名
    val tokenWords: DataFrame = token.transform(dat)
    tokenWords.show(false)

    //2.准备TF
    val hashTF: HashingTF = new HashingTF().setInputCol("token_words")
      .setOutputCol("tf_words")
    val tf_DF: DataFrame = hashTF.transform(tokenWords)
    //指定分好词的那一列

    //3.准备IDF
    val idf: IDF = new IDF().setInputCol("tf_words")
      .setOutputCol("idf_words")
    //指出要对那一列的数据进行计算IDF
    val model: IDFModel = idf.fit(tf_DF)

    //4.真正的计算结果
    val result: DataFrame = model.transform(tf_DF)
    result.show(false)
  }
}
