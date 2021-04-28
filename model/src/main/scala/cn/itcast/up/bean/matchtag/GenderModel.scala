package cn.itcast.up.bean.matchtag

import java.util
import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO
 *性别标签/模型/任务开发
 *
 * @param null
 * @return
 * @author ming
 * @date 2021/4/28 9:44
 */
object GenderModel {
  def main(args: Array[String]): Unit = {
    //0.准备saprk开发环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("model")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._

    //1.读取mysql数据
    val url: String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName: String = "tbl_basic_tag"
    val properties: Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)
    mysqlDF.show(false)

    //2.读取需要的性别标签星官的4级标签的rule并解析rule
    //2.1读取4级标签的rule
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=4")

    //2.2解析rule为map
    val kvtupleDS: Dataset[Array[(String, String)]] = fourRuleDS.map(row => {
      //将row转为string
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      val kvtuple: Array[(String, String)] = kvs.map(kvsStr => {
        val kv: Array[String] = kvsStr.split("=")
        (kv(0), kv(1))
      })
      kvtuple
    })
    val kvtupleLis: util.List[Array[(String, String)]] = kvtupleDS.collectAsList()
    val kvtuplearr: Array[(String, String)] = kvtupleLis.get(0)
    val fourRuleMap: Map[String, String] = kvtuplearr.toMap
    fourRuleMap.foreach(println)

    //3.根据解析好的4级标签的rule加载Hbase的数据
    //这里可以使用spark提供的原始的操作hbase的api
    //3.1准别Hbase 连接参数
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)
    //3.2使用自定义HBase数据源根据hbasemeta加载hbase数据
    val hbaseDF: DataFrame = spark.read.format("cn.itcast.up.tools.HBaseSource").option(HBaseMeta.INTYPE, hbaseMeta.inType).option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()
    hbaseDF.show(false)
    hbaseDF.printSchema()
    
    //4.读取MySQL中和性别标签相关的5级标签
    val fiveDS: Dataset[Row] = mysqlDF.select("id", "rule").where('pid===4)
    fiveDS.show(false)
    fiveDS.printSchema()

    //5.将hbaseDf和fiveDs进行匹配，得到如下数据
    //5.1将five转为map，方便后续udf操作
    val tmpDS: Dataset[(String, Long)] = fiveDS.as[(Long, String)].map(
      t => {
        (t._2, t._1)
      }
    )
    val tempArr: Array[(String, Long)] = tmpDS.collect()
    val fiveMap: Map[String, Long] = tempArr.toMap
    //5.2使用单表+UDF完成hbaseDF和fiveDS的匹配
    //自定义DSL风格的udf，将gender转为tagid
    import org.apache.spark.sql.functions._
    val gender2tagid = udf((gender: String) => {
      fiveMap(gender)
    })
    val resultDF: DataFrame = hbaseDF.select('id as "userId", gender2tagid('gender) as "tagsId")
    resultDF.show(false)

    resultDF.write.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS,hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT,hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE,"test")
      .option(HBaseMeta.FAMILY,hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS,"userId,tagIds")
      .option(HBaseMeta.ROWKEY,"userId")
      .save()
  }

}
