package com.dksou.fitting.ml.service.serviceImpl.randomforest


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/3/23 0023.
 */
object RFClassModelBuild {

//  val log = Logger.getLogger(RFClassModelBuild.getClass)
  def rfClassModelBuild(inputPath: String, modelPath: String, numClasses: Int,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("RFClassModelBuild")

    val sc = new SparkContext(conf)


    val data = DKUtil2.forBuildData(sc, dataType, inputPath)

    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    //分类
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 17 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 8
    val maxBins = 100

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    if (numClasses == 2) {
      val metrics = new BinaryClassificationMetrics(predictionAndLabels)
      val auPR = metrics.areaUnderPR()
      val auROC = metrics.areaUnderROC()

      println("Area under ROC = " + auROC)
      println("Area under PR = " + auPR)
    }
    else if (numClasses > 2) {
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      val recall = metrics.recall

      println("precision = " + precision)
      println("recall = " + recall)
    }
    // Save and load model
    model.save(sc, modelPath)
    sc.stop();
  }



  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    RFClassModelBuild.rfClassModelBuild(args(0),args(1),args(2).toInt,args(3))
  }

}
