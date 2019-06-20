package com.dksou.fitting.ml.service.serviceImpl.nb


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/25 0025.
 */
object NBModelBuild {
//  val log = Logger.getLogger(NBModelBuild.getClass)
  def nbModelBuild(inputPath: String, modelPath: String,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("NBModelBuild")

    val sc = new SparkContext(conf)



    val data = DKUtil2.forBuildData(sc, dataType, inputPath)
    val splits = data.randomSplit(Array(0.8, 0.2), 11l)
    val trainData = splits(0)
    val testData = splits(1)

    val model = NaiveBayes.train(trainData)

    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    val recall = metrics.recall

    println("precision = " + precision)
    println("recall = " + recall)

    model.save(sc, modelPath)
    sc.stop();
  }



  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    NBModelBuild.nbModelBuild(args(0),args(1),args(2))
  }

}
