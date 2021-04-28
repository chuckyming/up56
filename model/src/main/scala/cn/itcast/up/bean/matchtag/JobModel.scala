package cn.itcast.up.bean.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO
 *职业标签/模型/任务开发
 *
 * @author ming
 * @date 2021/4/28 13:59
 */
object JobModel {
  def main(args: Array[String]): Unit = {
    //1.spark evironment
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("model")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //1.sql data
    val url: String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName: String = "tbl_basic_tag"
    val properties: Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)

    //2.读取和职业相关的4及标签rule并解析
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=7")

    //解析rule为map
    val fourRuleMpa: Map[String, String] = fourRuleDS.map(row => {
      val rowStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = rowStr.split("##")
      kvs.map(kvsstr => {
        val kv: Array[String] = kvsstr.split("=")
        (kv(0), kv(1))
      })
    }).collectAsList().get(0).toMap

    //3.根据4及标签加载HBase数据
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMpa)

    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()
    hbaseDF.show(false)
    hbaseDF.printSchema()

    //4.读取和性别i标签祥光的5及标签
    val fiveDS: Dataset[Row] = mysqlDF.select("id", "rule").where('pid===7)
    fiveDS.show(false)
    fiveDS.printSchema()

    //5.将哈白色df和fiveds进行匹配
    //5.1将fivedf转为map，方便后续自定义udf操作
    val tempDS: Dataset[(String, Long)] = fiveDS.as[(Long, String)].map(t => {
      (t._2, t._1)
    })
    val tempArr: Array[(String, Long)] = tempDS.collect()
    val fiveMap: Map[String, Long] = tempArr.toMap

    //5.2使用单表+UDF完成hbasedf和fiveds的匹配
    //自定义DSL风格的udf，将gender转为tagid
    import org.apache.spark.sql.functions._
    val job2tagid = udf((job:String)=>{
      fiveMap(job)
    })

    val resultDF: DataFrame = hbaseDF.select('id as "userId", job2tagid('job) as "tagsId")
    resultDF.show(false)

    //6.将userId,tagsId保存到HBase
    //注意:这里不能简单的直接将结果写入到HBase,因为如果直接写入的话会导致将HBase中的之前的标签如性别标签覆盖了
    //而我们想要的是既保留用户的性别标签也保留用户的职业标签
    //思考该如何去做?
    //方案1:不同类型的标签存储到不同的列族,同一个列族下不同的标签存储在不同的列,到达应该存储到哪一个列族/哪一个列,根据业务和场景进行划分
    //如:性别/年龄/职业放到人口属性列族下的三个不同的列中,消费周期/支付方式放到商业属性下的两个不同的列
    //(但是要求提前有划分标准,后续保存的时候严格的按照标准来)
    //方案2:直接将HBase画像结果表的历史数据和当前这一次的结果数据合并，最后将合并的结果覆盖存储
    //该方案简单一点,所以学习时直接使用它
    //(但是注意,该方案,当数量量很大,标签很多的时候,不方便管理,不方便后续的查询)

    //6.1查询HBase中原来的画像结果表
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()
    oldDF.show(false)

    //6.2将oldDF和newDf进行合并--左外连接
    //SQL方式newDF左外连接oldDF
    resultDF.createOrReplaceTempView("t_new")
    oldDF.createOrReplaceTempView("t_old")

    //自定义sql风格udf
    spark.udf.register("merge", (newsTagId:String, oldTagsId:String)=>{
      if (StringUtils.isBlank(newsTagId)){
        oldTagsId
      }else if (StringUtils.isBlank(oldTagsId)){
        newsTagId
      }else{
        //set可以去重
        val newArr: Array[String] = newsTagId.split(",")
        val oldArr: Array[String] = oldTagsId.split(",")
        val resultArr: Array[String] = newArr ++ oldArr
        val set: Set[String] = resultArr.toSet
        val result: String = set.mkString(",")
        result
      }
    })

    val sql: String =
      """
        |select n.userId,merge(n.tagsId,o.tagsId) as tagsId
        |from t_new n
        |left join t_old o
        |on n.userId = o.userId
        |""".stripMargin
    val resultdf: DataFrame = spark.sql(sql)

    resultdf.show(false)
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

}
