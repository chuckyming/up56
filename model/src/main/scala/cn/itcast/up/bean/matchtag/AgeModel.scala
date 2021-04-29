package cn.itcast.up.bean.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.tools.scalap.scalax.util.StringUtil

/**
 * TODO
 * 完成年龄段标签/模型的开发
 *
 * @author ming
 * @date 2021/4/29 9:48
 */
object AgeModel {
  def main(args: Array[String]): Unit = {
    //0.准别spark环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("model")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //1.读取mysql中的数据
    val url: String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName: String = "tbl_basic_tag"
    val prop: Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, prop)

    //2.读取和年龄段标签相关的4及标签rule并解析
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=14")

    //解析rule为map
    val fourRuleMap: Map[String, String] = fourRuleDS.map(row => {
      val rowStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = rowStr.split("##")
      kvs.map(kvStr => {
        val kv: Array[String] = kvStr.split("=")
        (kv(0), kv(1))
      })
    }).collectAsList().get(0).toMap

    //3.根据解析出来的rule读取hbase数据
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)
    val hbaseDF: DataFrame = spark.read.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()
    hbaseDF.show(false)
    hbaseDF.printSchema()

    //4.读取和年龄段标签相关的5级标签（根据4及标签的id作为pid查询）
    val fiveDS: Dataset[Row] = mysqlDF.select("id", "rule").where("pid = 14")
    fiveDS.show(false)
    fiveDS.printSchema()

    //5.根据hbase数据和5级标签数据进行匹配，得出userId，tagsId
    //5.1统一格式
    import org.apache.spark.sql.functions._
    val hbaseDF2: DataFrame = hbaseDF.select('id as "userId", regexp_replace('birthday, "-", "")as "birthday")
    hbaseDF2.show(false)

    //5.2将fiveDS拆分为（“tagsId”，“start”，“end")
    val fiveDF2: DataFrame = fiveDS.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (t._1, arr(0), arr(1))
    }).toDF("tagsId", "start", "end")
    fiveDF2.show(false)

    //5.3将hbaseDF2和fiveDF2直接join
    val newDF: DataFrame = hbaseDF2.join(fiveDF2)
      .where(hbaseDF2.col("birthday").between(fiveDF2.col("start")
      , fiveDF2.col("end")))
      .select(hbaseDF2.col("userId"), fiveDF2.col("tagsId"))
    newDF.show(false)

    //6.查询HBAse中的oldDF
    val oldDF: DataFrame = spark.read.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

    //7.合并newDF和oldDF
    //自定义DSL的UDF
    val merge: UserDefinedFunction = udf((newTagsId: String, oldTagsId: String) => {
      if (StringUtils.isBlank(newTagsId)) {
        oldTagsId
      } else if (StringUtils.isBlank(oldTagsId)) {
        newTagsId
      } else {
        val newArr: Array[String] = newTagsId.split(",")
        val oldArr: Array[String] = oldTagsId.split(",")
        val resultArr = newArr ++ oldArr
        val set: Set[String] = resultArr.toSet
        val result: String = set.mkString(",")
        result
      }
    })

    val resutlDF: DataFrame = newDF.join(
      oldDF,
      newDF.col("userId") === oldDF.col("userId"), //join条件
      "left"
    ).select(
      newDF.col("userId"),
      merge(newDF.col("tagsId"), oldDF.col("tagsId")) as "tagsId"
    )
    resutlDF.show(false)

    resutlDF.write
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS,hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT,hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE,"test")
      .option(HBaseMeta.FAMILY,hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS,"userId,tagIds")
      .option(HBaseMeta.ROWKEY,"userId")
      .save()
  }

}
