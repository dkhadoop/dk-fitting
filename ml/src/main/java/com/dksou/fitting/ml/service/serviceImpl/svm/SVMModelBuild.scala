package com.dksou.fitting.ml.service.serviceImpl.svm


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/3/25 0025.
 */
object SVMModelBuild {

//  val log = Logger.getLogger(SVMModelBuild.getClass)
  def svmModelBuild(inputPath: String, modelPath: String,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("SVMModelBuild")

    val sc = new SparkContext(conf)


    val data = DKUtil2.forBuildData(sc, dataType, inputPath)
    val splits = data.randomSplit(Array(0.8, 0.2), 11l)
    val trainData = splits(0)
    val testData = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(trainData, numIterations)

    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    //仅支持二分类
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val auPR = metrics.areaUnderPR()
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
    println("Area under PR = " + auPR)

    model.save(sc, modelPath)
    sc.stop();
  }





  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    SVMModelBuild.svmModelBuild(args(0),args(1),args(2))
  }


}
