package com.dksou.fitting.ml.service.serviceImpl.svm


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/25 0025.
 */
object SVMModelPredict {
//  val log = Logger.getLogger(SVMModelPredict.getClass)

  def svmModelPredict(inputPath: String, modelPath: String, outputPath: String,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("SVMModelPredict")

    val sc = new SparkContext(conf)

    val model = SVMModel.load(sc, modelPath)

    val data = DKUtil2.forPredictData(sc, dataType, inputPath)

    val predictionAndFeatures = data.map {
      features =>
        val prediction = model.predict(features)
        println(features + "\n---------->" + prediction)
        (prediction, features)
    }

    predictionAndFeatures.map(x => Array(x._2, x._1).mkString("--")).saveAsTextFile(outputPath)
    sc.stop();
  }




  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    SVMModelPredict.svmModelPredict(args(0),args(1),args(2),args(3))
  }

}
