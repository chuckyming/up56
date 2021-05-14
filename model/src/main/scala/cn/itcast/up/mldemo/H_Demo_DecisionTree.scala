package cn.itcast.up.mldemo

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StandardScaler, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * TODO
 *  决策树
 *
 * @author ming
 * @date 2021/5/14 10:28
 */
object H_Demo_DecisionTree {
  def main(args: Array[String]): Unit = {
    //0.执行环境
    val spark: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val csvDF: DataFrame = spark.read.csv("file:///G:\\data\\up\\ml\\iris_DecisionTree.csv")
    val irisStringDF: DataFrame = csvDF.toDF("Seqal_Length", "Seqal_Width", "Petal_Length", "Petal_Width", "Species")
    val irirsDF: DataFrame = irisStringDF.select(
      'Sepal_Length cast DoubleType,
      'Sepal_Width cast DoubleType,
      'Petal_Length cast DoubleType,
      'Petal_Width cast DoubleType,
      'Species
    )
    irirsDF.show(10, false)

    //1.特征工程--标签数值化
    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("Species") //指定对一列的字符串进行数值化
      .setOutputCol("Species_Indexer")
    val stringIndexerModel: StringIndexerModel = stringIndexer.fit(irirsDF)//指定数值化之后该列的列名

    //2.特征工程--特征向量化
    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"))
      .setOutputCol("features")

    //3.数据归一化
    val standaScaler: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("StandardScaler_features")
      .setWithMean(true)
      .setWithStd(true)

    //4.构建决策树分类器
    val decisionTreeClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("StandardScaler_features")
      .setLabelCol("Species_Indexer")
      .setPredictionCol("predict")

    //5.还原标签列
    val indexToString: IndexToString = new IndexToString().setInputCol("predict").setOutputCol("predict_String")
      .setLabels(stringIndexerModel.labels)

    //6.数据集划分为80%训练集，20%测试集
    val Array(trainSet, testSet) = irirsDF.randomSplit(Array(0.8, 0.2), 100)

    //7.构建pipeline管道将上述的复杂的机器学习六层串联起来获得pipeline管道对象
    val pipeline: Pipeline = new Pipeline().setStages(Array(stringIndexerModel, vectorAssembler, standaScaler, decisionTreeClassifier, indexToString))

    //8.使用训练集训练pipeline对象中的决策树模型
    val pipelineModel: PipelineModel = pipeline.fit(trainSet)

    //9.使用测试机进行测试预测结果
    val result: DataFrame = pipelineModel.transform(testSet)
    result.show(false)

    //10.查看决策过程
    val decisionTressClassificationModel: DecisionTreeClassificationModel = pipelineModel.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    val decisionTreestring: String = decisionTressClassificationModel.toDebugString
    println("决策树的决策过程如下：\n" + decisionTreestring)

    //11.评估模型的好坏
    //创建多分类评古器
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("Species_Indexer").setPredictionCol("predict").setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(result)
    println("模型再测试集上的正确路为" + accuracy)
  }
}
