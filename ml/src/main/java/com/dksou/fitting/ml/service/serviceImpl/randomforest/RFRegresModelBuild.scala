package com.dksou.fitting.ml.service.serviceImpl.randomforest


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/23 0023.
 */
object RFRegresModelBuild {

//  val log = Logger.getLogger(RFRegresModelBuild.getClass)
  def rfRegresModelBuild(inputPath: String, modelPath: String,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("RFRegresModelBuild")

    val sc = new SparkContext(conf)



    val data = DKUtil2.forBuildData(sc, dataType, inputPath)

    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    //回归
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 6
    val maxBins = 100
    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new RegressionMetrics(predictionAndLabels)
    val mae = metrics.meanAbsoluteError
    val mse = metrics.meanSquaredError
    val rmse = metrics.rootMeanSquaredError
    val r2 = metrics.r2

    println("mean absolute error = " + mae)
    println("mean squared error = " + mse)
    println("root mean squared error = " + rmse)
    println("coefficient of determination = " + r2)
    // Save and load model
    model.save(sc, modelPath)
    sc.stop();
  }



  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    RFRegresModelBuild.rfRegresModelBuild(args(0),args(1),args(2))
  }

}
