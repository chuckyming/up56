//package cn.itcast.up.tools
//
//import cn.itcast.up.common.HDFSUtils
//import cn.itcast.up.mldemo.PSMModel.spark
//import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import org.bouncycastle.jcajce.provider.symmetric.DESede.PBEWithSHAAndDES2Key
//
//import scala.collection.immutable
//
///**
// * TODO
// *
// * @author ming
// * @date 2021/5/11 15:06
// */
//object KMeansUtils {
//  def getNewDF(predictResultDF: DataFrame, indexAndTagsIdMap: Map[Int, Long]): DataFrame = {
//    import spark.implicits._
//    import scala.collection.JavaConversions._
//    import org.apache.spark.sql.functions._
//
//    //得出userId，tagsId
//    val predict2TagsId = udf((predict: Int) => {
//      indexAndTagsIdMap(predict)
//    })
//    val newDF: DataFrame = predictResultDF.select('userId, predict2TagsId('predict) as "tagsId")
//    newDF
//  }
//
//  def getIndexAndTagsIdMap(model: KMeansModel, fiveDS: Dataset[Row]): Map[Int, Long] = {
//    import spark.implicits._
//    import scala.collection.JavaConversions._
//    import org.apache.spark.sql.functions._
//    //将聚类中心编号和对应的RFE的和求出并按照RFE的和排序--》
//    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(index => {
//      (index, model.clusterCenters(index).toArray.sum)
//    }).sortBy(_._2).reverse
//
//    //将indexandsum和fiveds进行zip拉链
//    val fiveRuleArr: Array[(Long, String)] = fiveDS.as[(Long, String)].collect().sortBy(_._2)
//    val tuples: immutable.IndexedSeq[((Int, Double), (Long, String))] = indexAndSum.zip(fiveRuleArr)
//    //
//    val indexAndTagsIdMap: Map[Int, Long] = tuples.map(t => {
//      (t._1._1, t._2._1)
//    }).toMap
//    indexAndTagsIdMap
//  }
//
//  def getKMeansModel(path: String, k: Int, i: Int, vectorDF: DataFrame): KMeansModel = {
//    var model:KMeansModel = null
//    if (HDFSUtils.getInstance().exists(path)){
//      println("模型存在，直接加载并使用")
//      model = KMeansModel.load(path)
//    }else {
//      println("模型不存在，重新训练并保存")
//      model = new KMeans()
//        .setK(k)
//        .setSeed(100)
//        .setMaxIter(i)
//        .setFeaturesCol("feature")
//        .setPredictionCol("predict")
//        .fit(vectorDF)
//      model.save(path)
//    }
//    model
//
//  }
//
//  def selectK(vectorDF: DataFrame, ks: List[Int]): Int = {
//    5
//  }
//
//  def getVectorDF(PSMDF: Dataset[Row], inputCols: Array[String]): DataFrame = {
//    //特征向量化
//    val vectorDF: DataFrame = new VectorAssembler()
//      .setInputCols(inputCols)
//      .setOutputCol("feature")
//      .transform(PSMDF)
//    vectorDF
//  }
//
//}
