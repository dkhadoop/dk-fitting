package com.dksou.fitting.ml.service.serviceImpl.lr


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.classification.LogisticRegressionModel
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
object LRModelPredict {

//  val log = Logger.getLogger(LRModelPredict.getClass)
  def lrModelPredict(inputPath: String, modelPath: String, outputPath: String,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("LRModelPredict")

    val sc = new SparkContext(conf)


    val model = LogisticRegressionModel.load(sc, modelPath)
    val numFeatures = model.numFeatures

    val data = DKUtil2.forPredictData(sc, dataType, numFeatures, inputPath)

    val predictionAndFeatures = data.map {
      features =>
        val prediction = model.predict(features)
        //println(features+"\n---------->"+prediction)
        (prediction, features)
    }

    predictionAndFeatures.map(x => Array(x._2, x._1).mkString("--")).saveAsTextFile(outputPath)
    sc.stop();
  }


  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    LRModelPredict.lrModelPredict(args(0),args(1),args(2),args(3))
  }

}
