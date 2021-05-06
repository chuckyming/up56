package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/**
 * TODO
 * 完成统计型标签-消费周期标签、模型的开发
 * 获取用户最近一次消费时间举例今天的天数
 * 如：5天谴，一个月前消费的
 * 方便找出长时间未消费的用户，做用户流式预警/用户挽回
 * @author ming
 * @date 2021/5/6 21:28
 */
object CycleModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  //23消费周期
  override def getTagId(): Long = 23

  //该方法由子类实现，完成用户消费周期标签/模型的计算
  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {

    fiveDS.show(false)
    fiveDS.printSchema()
    //0.导入饮食转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //1.求用户最近一次消费时间
    //根据用户分组去finishTime去最大值即可
    val tempDF: DataFrame = hbaseDF.groupBy('memberId as "userId")
      .agg(max('finishTime) as "maxFinishTime")

    //2.求用户最近一次消费时间距离今天的天数
    //求当前使劲啊和maxfinishingtime时间的天数差
    val daysCoulmn: Column = datediff(date_sub(current_date(), 200), from_unixtime('maxFinishTime)) as "days"
    val tempDF2: DataFrame = tempDF.select('userId, daysCoulmn)
    tempDF2.show(false)

    //3.将fiveDS拆成：（tagsId，start，end）
    val fiveDS2: DataFrame = fiveDS.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (t._1, arr(0), arr(1))
    }).toDF("tagsId", "start", "end")
    fiveDS2.show(false)

    //4.将tempDF2和fiveDS2进行匹配
    val newDF: DataFrame = tempDF2.join(fiveDS2)
      .where(tempDF2.col("days").between('start, 'end))
      .select('userId, 'tagsId)
    newDF.show(false)

    newDF

  }



}
