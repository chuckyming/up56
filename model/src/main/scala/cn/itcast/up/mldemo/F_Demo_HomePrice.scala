package cn.itcast.up.mldemo


import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * TODO
 * 使用sparkMLlib-api对房价数据进行特征工程处理，并使用线性回归做房价预测
 *
 * @author ming
 * @date 2021/5/7 16:54
 */
object F_Demo_HomePrice {
//准备样例类封装数据
  case class Home(
                   mlsNum:Double,
                   city:String,
                   sqFt:Double,
                   bedrooms:Double,
                   bathroms:Double,
                   garage:Double,
                   age:Double,
                   acres:Double,
                   price:Double
                   )

  def main(args: Array[String]): Unit = {
    //0.evironment
    val cong: SparkConf = new SparkConf().setMaster("local[2]").setAppName("HomePrice")
    val sc: SparkContext = new SparkContext(cong)
    sc.setLogLevel("WARN")
    val fileRDD: RDD[String] = sc.textFile("file:///G:\\data\\up\\ml\\data\\homeprice.data")
    fileRDD.foreach(println)

    val homeRDD: RDD[Home] = fileRDD.map(line => {
      val arr: Array[String] = line.split("[|]")
      Home(
        arr(0).toDouble,
        arr(1).toString,
        arr(2).toDouble,
        arr(3).toDouble,
        arr(4).toDouble,
        arr(5).toDouble,
        arr(6).toDouble,
        arr(7).toDouble,
        arr(8).toDouble
      )
    })

    //1.数据向量化--先将价格向量化
    val priceVector: RDD[linalg.Vector] = homeRDD.map(home => {
      Vectors.dense(home.price)
    })

    //2.查看价格的统计学特征
    val summary: MultivariateStatisticalSummary = Statistics.colStats(priceVector)
    println("max:" + summary.max)
    println("min:" + summary.min)
    println("mean:" + summary.mean)
    println("variance:" + summary.variance)

    //3.过滤掉一场数据--也可以再次统计学特征
    //过滤出的一场数据
    val falseData: RDD[Home] = homeRDD.filter(home => home.price > 100000000 || home.price < 2 || home.sqFt < 2 || home.sqFt > 100000)
    println("falseDAta")
    println(falseData.count())
    //过滤出正常的数据
    val trueData: RDD[Home] = homeRDD.filter(home => (home.price >2 && home.price < 100000000) || (home.sqFt > 2 && home.sqFt < 100000))
    println("trueData")
    println(trueData.count())

    //4.查看价格和面积的相关系数
    val corr: Double = Statistics.corr(trueData.map(home => home.price), trueData.map(home => home.sqFt))
    println(corr)

    //5.将数据转为标签向量LabeledPoint
    val labeledPointRDD: RDD[LabeledPoint] = trueData.map(home => {
      LabeledPoint(home.price, Vectors.dense(home.age, home.bathroms, home.bedrooms, home.garage, home.sqFt))
    })

    //6.对数据进行归一化
    val model: StandardScalerModel = new StandardScaler(true, true).fit(labeledPointRDD.map(lp=>lp.features))
    val scalerRDD: RDD[LabeledPoint] = labeledPointRDD.map(lp => {
      LabeledPoint(lp.label, model.transform(lp.features))
    })
    scalerRDD.take(5).foreach(println)
    
    //7.拓展：使用线性回归训练模型
    val linearRegressionWithSGD: LinearRegressionWithSGD = new LinearRegressionWithSGD().setIntercept(true)
    linearRegressionWithSGD.optimizer.setNumIterations(1000).setStepSize(0.1)
    //划分数据集
    val Array(testset, trainset) = scalerRDD.randomSplit(Array(0.3, 0.7), 100)
    //训练模型
    val linearRegressionModel: LinearRegressionModel = linearRegressionWithSGD.run(trainset)

    //8.扩展：评价模型
    val priceTuple: RDD[(Double, Double)] = trainset.map(lp => {
      val predictPrice: Double = linearRegressionModel.predict(lp.features)
      //返回
      (lp.label, predictPrice)
    })
    val powerSum: Double = priceTuple.map(t => {
      math.pow(t._1 - t._2, 2)
    }).reduce(_ + _)
    //mse:均方误差
    val mse: Double = powerSum/priceTuple.count()
    println("在训练集上的均方误差为：" + mse)

    //9.扩展：使用模型对测试机做预测
    val priceTuple2: RDD[(Double, Double)] = testset.map(lp => {
      val predictPrice: Double = linearRegressionModel.predict(lp.features)
      (lp.label, predictPrice)
    })
    val powerSum2: Double = priceTuple2.map(t => {
      math.pow(t._1 - t._2, 2)
    }).reduce(_ + _)
    val mse2: Double = powerSum2/priceTuple2.count()
    println("在测试集上的均方误差为"+ mse2)

    priceTuple2.take(5).foreach(println)
  }
}
