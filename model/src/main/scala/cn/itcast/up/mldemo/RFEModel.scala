package cn.itcast.up.mldemo

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.{immutable, mutable}
/**
 * TODO
 *
 * @author ming
 * @date 2021/5/10 14:52
 */
object RFEModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  //45活跃度
  override def getTagId(): Long = 45

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    //0.导入隐式转换
    import spark.implicits._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._

    val recencyStr: String = "recency"
    val frequencyStr: String = "frequency"
    val engagementsStr: String = "engagements"
    val featureSTr: String = "feature"
    val scaleFeatureStr: String = "scaleFeature"
    val predictStr: String = "predict"

    //1.计算用户的RFE
    val recencyColum: Column = datediff(date_sub(current_date(), 206), max('log_time)) as recencyStr
    val frequencyColumn: Column = count('loc_url) as frequencyStr
    val engagementsColumn: Column = countDistinct('loc_url) as engagementsStr
    //1.2分组计算RFE
    val RFEDF: DataFrame = hbaseDF.groupBy('global_user_id as "userId")
      .agg(recencyColum, frequencyColumn, engagementsColumn)

    //2.数据归一化/标准化
    val recencyScore: Column = when(col(recencyStr).between(0, 15), 5)
      .when(col(recencyStr).between(16, 30), 4)
      .when(col(recencyStr).between(31, 45), 3)
      .when(col(recencyStr).between(46, 60), 2)
      .when(col(recencyStr).gt(60), 1)
      .as(recencyStr)

    val frequencyScore: Column = when(col(frequencyStr).geq(400), 5)
      .when(col(frequencyStr).between(300, 399), 4)
      .when(col(frequencyStr).between(200, 299), 3)
      .when(col(frequencyStr).between(100, 199), 2)
      .when(col(frequencyStr).leq(99), 1)
      .as(frequencyStr)

    val engagementScore: Column = when(col(engagementsStr).geq(250), 5)
      .when(col(engagementsStr).between(200, 249), 4)
      .when(col(engagementsStr).between(150, 199), 3)
      .when(col(engagementsStr).between(50, 149), 2)
      .when(col(engagementsStr).leq(99), 1)
      .as(engagementsStr)

    //对于日志表过滤掉可能为空的值，因为spakrmllib的api不支持null
    val RFEScoreDF: DataFrame = RFEDF.select('userId, recencyScore, frequencyScore, engagementScore)
      .where('userId.isNotNull && 'recency.isNotNull && 'frequency.isNotNull && 'engagements.isNotNull)

    //3.特征向量化
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, engagementsStr))
      .setOutputCol(featureSTr)
      .transform(RFEScoreDF)

    //5.模型的加载或训练再保存
    val path: String = "/model/RFE35"
    var model: KMeansModel = null
    if (HDFSUtils.getInstance().exists(path)){
      println("模型存在，直接加载并使用")
      model = KMeansModel.load(path)
    }else {
      println("模型不存在， 重新训练并保存")
      model = new KMeans()
          .setK(4)
          .setSeed(100)
          .setMaxIter(20)
          .setFeaturesCol(featureSTr)
          .setPredictionCol(predictStr)
          .fit(vectorDF)
      model.save(path)
    }

    //6.预测
    val predictResultDF: DataFrame = model.transform(vectorDF)
    predictResultDF.show(false)

    //7.将聚类中心彪悍和5及标签id对应起来
    //7.1将据类中心编号和u翟营的RFE的和求出并按照RFE的和排序
    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(index => {
      (index, model.clusterCenters(index).toArray.sum)
    }).sortBy(_._2).reverse

    //7.2将indexAndSum和fiveDS进行zip拉链
    val fiveRuleARR: Array[(Long, String)] = fiveDS.as[(Long, String)].collect().sortBy(_._2)
    val tuples: immutable.IndexedSeq[((Int, Double), (Long, String))] = indexAndSum.zip(fiveRuleARR)
    val indexAndTagsIdMap: Map[Int, Long] = tuples.map(t => {
      (t._1._1, t._2._1)
    }).toMap

    //8.得出userId，tagsId
    val predict2TagsId: UserDefinedFunction = udf((predict: Int) => {
      indexAndTagsIdMap(predict)
    })
    val newDF: DataFrame = predictResultDF.select('userId, predict2TagsId('predict) as "tagsId")
    newDF.show(false)

    null

  }
}
