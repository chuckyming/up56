package cn.itcast.up.mldemo

import cn.itcast.up.base.BaseModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}

/**
 * TODO
 *
 * @author ming
 * @date 2021/5/8 10:12
 */
object RFMModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  //37 客户价值
  override def getTagId(): Long = 37

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    hbaseDF.show(false)
    hbaseDF.printSchema()
    fiveDS.show( false)
    fiveDS.printSchema()

    //0.evironment
    import spark.implicits._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._

    //1.定义常量
    val recencySTr: String = "recency"
    val frequencyStr: String = "frequency"
    val monetaryStr: String = "monetary"
    val featureStr: String = "feature"
    val predictStr: String = "predict"

    //2:通过以上数据我们就可以求出每个用户的rfm：按照用户id分组后求：
    //r:recency最近一次消费四件距离今天的天数
    //f:grequency最近一段时间消费次数
    //m:monetary最近一段时间消费总金额
    val recencyColum:Column = datediff(date_sub(current_date(), 206), from_unixtime(max('finishTime))) as recencySTr
    val frequencyColumn: Column = count('orderSn) as frequencyStr
    val monetaryColumn: Column = sum('orderAmount) as monetaryStr

    //1.2按照用户id分区求RFM
    val RFMDF: DataFrame = hbaseDF.groupBy('memberId as "userId")
      .agg(recencyColum, frequencyColumn, monetaryColumn)
    RFMDF.show(false)

    //2.数据归一化/标准化
    val recencyScore: Column = functions.when(col(recencySTr) >= 1 && col(recencySTr) <= 3,5)
      .when(col(recencySTr) >=4 && col(recencySTr) <=6,4)
      .when(col(recencySTr) >=7 && col(recencySTr) <=9,3)
      .when(col(recencySTr) >=10 && col(recencySTr) <=15,2)
      .when(col(recencySTr) >=16,1)
      .as(recencySTr)
    val frequencyScore: Column = functions.when(col(frequencyStr) >= 200, 5)
      .when((col(frequencyStr) >= 150) && (col(frequencyStr) <= 199),4)
      .when((col(frequencyStr) >= 100) && (col(frequencyStr) <= 149),3)
      .when((col(frequencyStr) >= 50) && (col(frequencyStr) <= 99),2)
      .when((col(frequencyStr) >= 1) && (col(frequencyStr) <= 49),1)
      .as(frequencyStr)
    val monetaryScore: Column = functions.when(col(monetaryStr) >= 200000, 5)
      .when(col(monetaryStr).between(100000, 199999), 4)
      .when(col(monetaryStr).between(50000, 99999), 3)
      .when(col(monetaryStr).between(10000, 49999), 2)
      .when(col(monetaryStr) <= 9999, 1)
      .as(monetaryStr)
    val RMFScoreDF: DataFrame = RFMDF.select('userId, recencyScore, frequencyScore, monetaryScore)
    RMFScoreDF.show(false)
    RMFScoreDF.printSchema()

    //3.特征向量话
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencySTr, frequencyStr, monetaryStr))
      .setOutputCol(featureStr)
      .transform(RMFScoreDF)
    vectorDF.show(false)
    vectorDF.printSchema()

    //4.训练KMeans模型
    val model: KMeansModel = new KMeans()
      .setK(7)
      .setSeed(200)
      .setMaxIter(20)
      .setFeaturesCol(featureStr)
      .setPredictionCol(predictStr)
      .fit(vectorDF)

    //5.预测
    val predictResultDF: DataFrame = model.transform(vectorDF)
    predictResultDF.show(false)

    //6.进一步查看每个据类中心的RFM的和的最大值最小值，并按照据类中心索引编号排序
    val RFMClusterInfo: DataFrame = predictResultDF.groupBy(predictStr).agg(
      max(col(recencySTr) + col(frequencyStr) + col(monetaryStr)),
      min(col(recencySTr) + col(frequencyStr) + col(monetaryStr))
    ).sort(col(predictStr))
    RFMClusterInfo.show(false)

    //7.将预测出的用户据类中心索引编号和用户价值等级id对应
    null


  }

}
