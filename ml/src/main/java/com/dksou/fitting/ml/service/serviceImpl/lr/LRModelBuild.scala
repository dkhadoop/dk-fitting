package com.dksou.fitting.ml.service.serviceImpl.lr


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/22 0022.
 */
object LRModelBuild {
//  val log = Logger.getLogger(LRModelBuild.getClass)

  def lrModelBuild(inputPath: String, modelPath: String, numClass: Int,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("LRModelBuild")

    val sc = new SparkContext(conf)


    val data = DKUtil2.forBuildData(sc, dataType, inputPath)

    //    if (dataType == "LibSVM") {
    //        data= MLUtils.loadLibSVMFile(sc, inputPath)
    //      }
    //    else if (dataType == "LabeledPoints") {
    //        val text = sc.textFile(inputPath)
    //        data=text.map { line =>
    //          val parts = line.split(",")
    //          LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(java.lang.Double.parseDouble)))
    //        }
    //      }


    // val data = MLUtils.loadLabeledPoints(sc,inputPath)


    val splits = data.randomSplit(Array(0.8, 0.2), 11l)
    val trainData = splits(0)
    val testData = splits(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(numClass).run(trainData)

    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    if (numClass == 2) {
      val metrics = new BinaryClassificationMetrics(predictionAndLabels)
      val auPR = metrics.areaUnderPR()
      val auROC = metrics.areaUnderROC()

      println("Area under ROC = " + auROC)
      println("Area under PR = " + auPR)
    }
    else if (numClass > 2) {
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      val recall = metrics.recall

      println("precision = " + precision)
      println("recall = " + recall)
    }

    model.save(sc, modelPath)
    sc.stop();
  }




  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    LRModelBuild.lrModelBuild(args(0),args(1),args(2).toInt,args(3))
  }


}
