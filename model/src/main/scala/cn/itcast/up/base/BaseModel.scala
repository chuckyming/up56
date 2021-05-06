package cn.itcast.up.base

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO
 * desc trait相当于java中的借口，但是支持普通方法（可以被子类/实现类继承），还支持抽宪法昂发（可以让子类重写/实现）
 * 可以将重复的代码封装在trait的普通方法中，将不一样的步骤/代码做出抽象方法让子类/实现类去重写/实现
 *模板方法设计模式
 *
 * @author ming
 * @date 2021/4/29 15:53
 */
trait BaseModel {
//声明一个抽象方法，由子类提供具体的方法实现
  def getTagId():Long
  //抽象方法，由子类提供具体的方法实现
  def compute(hbaseDF: DataFrame, fiveDS:Dataset[Row]):DataFrame

  //0。准备spark开发环境
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("model").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  //导入隐式转换
  import spark.implicits._

  def getMySQLData(): DataFrame = {
    val url:String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val prop: Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, prop)
    mysqlDF
  }

  def getFourRule(mysqlDF:DataFrame, id:Long):HBaseMeta = {
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where('id === id)
    //解析rule为map
    val fourRuleMap = fourRuleDS.map(row => {
      val rowStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = rowStr.split("##")
      kvs.map(kvStr => {
        val kv: Array[String] = kvStr.split("=")
        (kv(0), kv(1))
      })
    }).collectAsList().get(0).toMap
    val hBaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)
    hBaseMeta
  }

  def getHbaseDF(hbaseMeta: HBaseMeta):DataFrame = {
    spark.read.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()
  }

  def getFiveRuleDF(mysqlDF:DataFrame, id:Long):Dataset[Row] ={
    mysqlDF.select("id", "rule").where('pid ===id)
  }

  def getHBaseOldDF(hbaseMeta:HBaseMeta):DataFrame ={
    spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()
  }

  def merge(newDF:DataFrame, oldDF:DataFrame):DataFrame = {
    //自定义DSL风格的UDF

    val merge: UserDefinedFunction = udf((newTagesId: String, oldTagsId: String) => {
      if (StringUtils.isBlank(newTagesId)) {
        oldTagsId
      } else if (StringUtils.isBlank(oldTagsId)) {
        newTagesId
      } else {
        //set可以去重
        val newArr: Array[String] = newTagesId.split(",")
        val oldArr: Array[String] = oldTagsId.split(",")
        val resultArr: Array[String] = newArr ++ oldArr
        val set: Set[String] = resultArr.toSet
        val result: String = set.mkString(",")
        result
      }
    })
    val resultDF: DataFrame = newDF.join(
      oldDF,
      newDF.col("userId") === oldDF.col("userId"), //join条件
      "left"
    ).select(
      newDF.col("userId"),
      merge(newDF.col("tagsId"), oldDF.col("tagsId")) as "tagsId"
    )
    resultDF
  }

  def save2HBase(resultDF:DataFrame, hbaseMeta:HBaseMeta): Unit ={
    resultDF.write
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS,hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT,hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE,"test")
      .option(HBaseMeta.FAMILY,hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS,"userId,tagIds")
      .option(HBaseMeta.ROWKEY,"userId")
      .save()
  }

  //现在execute方法中的代码太多了，而且第五步应该由子类来实现，所以execute方法中不应该直接全写完，儿应该将第五步留给子类实现
  //其他步骤全写在execute方法中也太多了，所以可以在封装为方法，或提前出去
  def execute():Unit ={
    println("execute方法执行了，一次执行1~8步，遇到抽象的执行子类的")
    //1.读取mysql中的数据
    val mysqlDF: DataFrame = getMySQLData()

    //2.读取模型/标签相关的4级标签rule并解析--标签id不一样
    val id: Long = getTagId()
    val hBaseMeta: HBaseMeta = getFourRule(mysqlDF, id)

    //3.根据解析出来的rule读取HBase数据
    val hbaseDF: DataFrame = getHbaseDF(hBaseMeta)

    //4.读取模型、标签相关的5及标签（根据4及标签的id作为pid查询）--标签id不一样
    val fiveDs: Dataset[Row] = getFiveRuleDF(mysqlDF, id)

    //5.根据Hbase数据和5及标签数据进行匹配，得出userId，tagsId
    val newDF: DataFrame = compute(hbaseDF, fiveDs)

    //6.查询HBase中的oldDF
    val oldDF: DataFrame = getHBaseOldDF(hBaseMeta)

    //7.合并newDF和oldDF
    val resultDF: DataFrame = merge(newDF, oldDF)

    //8.将最终结果写入到HBase
    save2HBase(resultDF, hBaseMeta)
  }
}
