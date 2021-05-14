package cn.itcast.up.mldemo

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import cn.itcast.up.tools.KMeansUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.{immutable, mutable}

/**
 * TODO
 * price sensitivity measurement价格蜜柑都模型
 * psm = 优惠订单占比+ 平均优惠金额占比+ 优惠总金额占比
 *
 * @author ming
 * @date 2021/5/11 9:17
 */
object PSMModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  override def getTagId(): Long = 50

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    hbaseDF.show(false)
    hbaseDF.printSchema()
    fiveDS.show(false)
    fiveDS.printSchema()

    import spark.implicits._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._

    //1.计算每个用户的PSM
    //1应收金额 = 实际订单金额 + 优惠金额
    val raColumn: Column = 'orderAmount + 'couponCodeValue as "ra"
    //折扣金额/优惠金额
    val dacolumn: Column = 'couponCodeValue as "da"
    //实收金额
    val pacolumn: Column = 'orderAmount as "pa"
    //优惠订单状态为1，非优惠订单状态为0
    val statecolumn: Column = when('couponCodeValue =!= 0, 1)
      .when('couponCodeValue === 0, 0)
      .as("state")

    //1.2执行查询计算用户的‘ra， da， pa， state
    val tempDF: DataFrame = hbaseDF.select('memberId as "userId", raColumn, dacolumn, pacolumn, statecolumn)
    tempDF.show(false)

    //1.3计算每个用户的优惠订单数、总订单数、优惠总金额，应收总金额
    val tdoncolumn: Column = sum('state) as "tdon"
    val toncolumn: Column = count('state) as "ton"
    val tdacolumn: Column = sum('da) as "tda"
    val tracolumn: Column = sum('ra) as "tra"
    val tempDF2: DataFrame = tempDF.groupBy('userId)
      .agg(tdoncolumn, toncolumn, tdacolumn, tracolumn)
    tempDF2.show(false)

    //1.4计算每个用的PSM
    val psmColumn:Column = ('tdon/'ton) + (('tda/'tdon)/('tra/'ton)) + ('tda/'tra)  as psmScoreStr

    val PSMDF: Dataset[Row] = tempDF2.select('userId, psmColumn)
      .filter('psm.isNotNull)
    PSMDF.show(false)

    //2.使用KMeans算法据类
    //有些步骤可以封装抽取，思考小如何封装抽取
    //1.特征向量化
    val inputCols: Array[String] = Array("psm")
    val vectorDF: DataFrame = KMeansUtils.getVectorDF(PSMDF, inputCols)

    //2.选取k值
    println("k值选取开始")
    val ks: List[Int] = List(2,3,4,5,6,7,8,9)
    val k: Int = KMeansUtils.selectK(vectorDF, ks)
    println("k值选取结束")

    //3.模型加载或训练再保存
    val path: String = "/model/PSM35"
    val model: KMeansModel = KMeansUtils.getKMeansModel(path, k, 20, vectorDF)

    //4.预测
    val predictResultDF: DataFrame = model.transform(vectorDF)
    predictResultDF.show(false)

    //5.据类中心标号和tagsId相对应
    val indexAndTagsIdMap: Map[Int, Long] = KMeansUtils.getIndexAndTagsIdMap(model, fiveDS)

    //6.获取userId和tagsId
    val newDF: DataFrame = KMeansUtils.getNewDF(predictResultDF, indexAndTagsIdMap)
    newDF.show(false)

    null

  }

}
