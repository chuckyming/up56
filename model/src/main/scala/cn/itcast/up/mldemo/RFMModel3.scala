package cn.itcast.up.mldemo

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import cn.itcast.up.mldemo.RFMModel2.spark
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}

import scala.collection.{immutable, mutable}

/**
 * TODO
 *
 * @author ming
 * @date 2021/5/10 14:28
 */
object RFMModel3 extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  //37客户价值
  override def getTagId(): Long = 37

  override def compute(hbaseDF: DataFrame, fiveDS: Dataset[Row]): DataFrame = {
    hbaseDF.show(false)
    hbaseDF.printSchema()
    fiveDS.show(false)
    fiveDS.printSchema()

    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //0.定义常量
    val recencyStr: String = "recency"
    val frequencyStr: String = "frequency"
    val monetaryStr: String = "monetary"
    val featureStr: String = "feature"
    val predictStr: String = "predict"

    //1.1申明Column兑现表示RFM如何计算
    val recencyColumn: Column = datediff(date_sub(current_date(), 206), from_unixtime(max('finishTime))) as recencyStr
    val frequencyColumn: Column = count('orderSn) as frequencyStr
    val monetaryColumn: Column = sum('orderAmount) as monetaryStr

    //1.2按照用户id分组求RFM
    val RFMDF: DataFrame = hbaseDF.groupBy('memberId as "userId")
      .agg(recencyColumn, frequencyColumn, monetaryColumn)
    RFMDF.show(false)

    //2.数据归一化/标准化
    val recencScore: Column = functions.when(col(recencyStr) >= 1 && col(recencyStr) <= 3, 5)
      .when(col(recencyStr) >= 4 && col(recencyStr) <= 6, 4)
      .when(col(recencyStr) >= 7 && col(recencyStr) <= 9, 3)
      .when(col(recencyStr) >= 10 && col(recencyStr) <= 15, 2)
      .when(col(recencyStr) >= 16, 1)
      .as(recencyStr)
    val frequencyScore: Column = functions.when(col(frequencyStr) >= 200, 5)
      .when(col(frequencyStr) >= 150 && col(frequencyStr) <= 199, 4)
      .when(col(frequencyStr) >= 100 && col(frequencyStr) <= 149, 3)
      .when(col(frequencyStr) >= 50 && col(frequencyStr) <= 99, 2)
      .when(col(frequencyStr) >= 1 && col(frequencyStr) <= 49, 1)
      .as(frequencyStr)
    val monetaryScore: Column = functions.when(col(monetaryStr) >= 200000, 5)
      .when(col(monetaryStr).between(100000, 199999), 4)
      .when(col(monetaryStr).between(50000, 99999), 3)
      .when(col(monetaryStr).between(10000, 49999), 2)
      .when(col(monetaryStr) <= 9999, 1)
      .as(monetaryStr)

    val RDMScoreDF: DataFrame = RFMDF.select('userId, recencScore, frequencyScore, monetaryScore)
    RDMScoreDF.show(false)
    RDMScoreDF.printSchema()

    //3.特征向量化
    //使用VectorAssembler可以将多列值装配为一个向量
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, monetaryStr))
      .setOutputCol(featureStr)
      .transform(RDMScoreDF)
    vectorDF.show(false)
    vectorDF.printSchema()

    // 肘部法进行k值选取
    println("k值选取开始")
    //1.准备一些备选的K如3~9
    //准备一个集合用来存放K
    val ks: List[Int] = List(3, 4, 5, 6,7,8,9)
    //准备一个集合用来存放k和对应的SSE
    val map: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
    //2.然后进行模型的训练，并计算各个看、K值对应的SSE
    for (k <- ks){
      val model: KMeansModel = new KMeans()
        .setK(k)
        .setSeed(200)
        .setMaxIter(20)
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .fit(vectorDF)
      //获取k值对应的SSE
      val sse: Double = model.computeCost(vectorDF)
      map.put(k, sse)
    }
    //3.打印这些K值对应的SSE的值，作图选取”拐点“对应的K
    val kAnddSSEArr: Array[(Int, Double)] = map.toArray.sortBy(_._1)
    kAnddSSEArr.foreach(println)
    println("k值选取结束")

    //0.声明模型存储的位置
    val path: String = "/model/RFM35"
    //0.声明模型
    var model: KMeansModel = null

    //1.判断HDFS上是否由该模型/路径
    if  (HDFSUtils.getInstance().exists(path)){
      println("模型存在于HDFS,直接加载并使用")
      model = KMeansModel.load(path)
    }else {//路径模型不存在，重新训练并保存到HDFS
      println("模型不存在，重新训练并保存")
      model = new KMeans()
        .setK(7)
        .setSeed(200)
        .setMaxIter(20)
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .fit(vectorDF)
      model.save(path)
    }

    //5.预测
    val predictResultDF: DataFrame = model.transform(vectorDF)
    predictResultDF.show(false)

    //6进一步查看每个据类中心的RFM的和的最大值和最小值，并按照据类中心索引编号顺序
    //7.将据类中心索引编号和5及标签对应起来
    //通过分析知道，据类中心的RFM的sum越大说明该据类中心的值越大，那么根据据类的原理
    //聚到该类/族的其他用户应该和据类中心是很详尽的也是价值较大的
    //7.1将据类中心的RFM的sum按照从大到小排序，得出按sum排序后的predict聚类中心索引编号，据类中心的RFM的sum
    val centers: Array[linalg.Vector] = model.clusterCenters //获取所有聚类中心
    val indices: Range = centers.indices//获取据类中心的索引编号组成的集合

    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = indices.map(index => {
      val center: linalg.Vector = centers(index) //根据索引去耗中心
      val RFMSum: Double = center.toArray.sum
      (index, RFMSum)
    }).sortBy(_._2).reverse
    indexAndSum.foreach(println)

    //7.2使用indexAndSum和fiveDS得出据类中心索引编号和5级标签tagsId的对应关系，即
    //将indexandsum和fiveds zip拉链即可
    val fiveRuleArr: Array[(Long, String)] = fiveDS.as[(Long, String)].collect().sortBy(_._2)
    val tuples: immutable.IndexedSeq[((Int, Double), (Long, String))] = indexAndSum.zip(fiveRuleArr)
    //取出（predictIndex，tagsId）
    val indexAndTagsIdMap: Map[Int, Long] = tuples.map(t => {
      (t._1._1, t._2._1)
    }).toMap
    indexAndTagsIdMap.foreach(println)

    //8.取出predictResultDF中的userId和predict，并将predict转为tagsId，最后得出userId，tagsId
    val predict2tagsId: UserDefinedFunction = udf((predict: Int) => {
      indexAndTagsIdMap(predict)
    })
    val newDF: DataFrame = predictResultDF.select('userId, predict2tagsId('predict) as "tagsId")
    newDF.show(false)

    null
  }

}
