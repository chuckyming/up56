package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * TODO
 *完成统计行标签-支付方式标签/模型的开发
 * 统计用户最常用的支付方式，然后给用户打赏响应的标签
 * @author ming
 * @date 2021/5/6 21:56
 */
object PaymentModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  //29 支付方式
  override def getTagId(): Long = 29

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    hbaseDF.show(false)
    fiveDS.show(false)

    //0。导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1.根据用户id+支付方式进行分组并计数
    val tempDF: DataFrame = hbaseDF.groupBy('memberId, 'paymentCode)
      .agg(count('paymentCode) as "counts")
      .select('memberId as "userId", 'paymentCode, 'counts)
    tempDF.show(false)

    //2.使用开窗函数进行组内排序，取top1（每个用户使用最多的付款方式）
    //1：DSL风格开窗函数
    val tempDF2: DataFrame = tempDF.withColumn("rn", row_number().over(Window.partitionBy('userId).orderBy('counts.desc)))
    val rankDF: DataFrame = tempDF2.where('rn ===1)

    //方式2：SQL风格开窗函数
    tempDF.createOrReplaceTempView("t_temp")
    val sql: String =
      """
        |select userId, paymentCode,counts,
        |row_number() over(partition by userId order by counts desc) rn
        |from t_temp
        |""".stripMargin
    val tempDF3: DataFrame = spark.sql(sql)
    val rankDF2: Dataset[Row] = tempDF3.where('rn ===1)
    rankDF2.show(false)

    //3.将rankDf和fiveDs进行匹配
    //3.1将fiveDs转为map
    //fiveDS.as[(tagsId,支付方式)]
    //fiveMap：Map[支付方式，tagsId]
    val fiveMap: Map[String, Long] = fiveDS.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    //3.2将rankDf中的支付方式换成tagsId
    val paymentCode2tagsId: UserDefinedFunction = udf((paymentCode: String) => {
      //如果支付方式获取到了直接返回对应的tagsId
      //如果支付方式没有获取到，那么返回-1，
      val tagsId: Long = fiveMap.getOrElse(paymentCode, -1)
      if (tagsId == -1) {
        val tagsId: Long = fiveMap("other")
      }
      tagsId
    })
    val newDF: DataFrame = rankDF.select('userId, paymentCode2tagsId('paymentCode) as "tagsId")
    newDF.show(false)

    null




  }


}
