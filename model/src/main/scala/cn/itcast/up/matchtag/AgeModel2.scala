package cn.itcast.up.matchtag

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * TODO
 *
 * @author ming
 * @date 2021/4/30 14:40
 */
object AgeModel2 extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
    //调用父类集成的execute方法
  }

  override def getTagId(): Long = 14

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    println("执行子类的compute方法")
    //5.1 统一格式，将1999-09-09统一为：19990909
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hbaseDF2: DataFrame = hbaseDF.select('id as "userId", regexp_replace('birthday, "-","") as "birthday")
    hbaseDF2.show(false)

    //5.2将fiveDS拆分为（“tagsId”，“start", "end")
    //fiveDF。
    val fiveDS2: DataFrame = fiveDS.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (t._1, arr(0), arr(1))
    }).toDF("tagsId", "start", "end")
    fiveDS2.show(false)

    //5.3将hbaseDF2和fiveDS2直接join
    val newDF: DataFrame = hbaseDF2.join(fiveDS2)
      .where(hbaseDF2.col("birthday").between(fiveDS2.col("start"), fiveDS2.col("end")))
      .select(hbaseDF2.col("userId"), fiveDS2.col("tagsId"))
    newDF.show(false)

    newDF
  }

}
