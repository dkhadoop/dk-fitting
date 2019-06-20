package com.dksou.fitting.ml.service.serviceImpl.nb


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.classification.NaiveBayesModel
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
object NBModelPredict {

//  val log = Logger.getLogger(NBModelPredict.getClass)
  def nbModelPredict(inputPath: String, modelPath: String, outputPath: String,dkmllibpath:String, dataType: String = "LabeledPoints"): Any = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("NBModelPredict")

    val sc = new SparkContext(conf)

    val model = NaiveBayesModel.load(sc, modelPath)

    val numFeatures = DKUtil2.getNumFeatures(sc, modelPath)
    //println("numFeatures = " + numFeatures)

    val data = DKUtil2.forPredictData(sc, dataType, numFeatures, inputPath)

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
    NBModelPredict.nbModelPredict(args(0),args(1),args(2),args(3))
  }


}
