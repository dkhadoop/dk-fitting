package com.dksou.fitting.ml.service.serviceImpl.gaussian

import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/24 0024.
 */
object GMModelPredict {
//  val log = Logger.getLogger(GMModelPredict.getClass)
  def gmModelPredict(inputPath:String,modelPath:String,outputPath:String,dkmllibpath:String,dataType:String = "LabeledPoints"): Unit = {

    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("GMModelPredict")

    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.parquetFile(new Path(modelPath, "data").toUri.toString)
    //    val dataArray = dataFrame.select("weight", "mu", "sigma").collect()
    //
    //    val (weights, gaussians) = dataArray.map {
    //      case Row(weight: Double, mu: Vector, sigma: Matrix) =>
    //        (weight, new MultivariateGaussian(mu, sigma))
    //    }.unzip

    val dataArray = dataFrame.select("weight", "mu", "numRows", "numCols", "sigma").collect()

    val (weights, gaussians) = dataArray.map {
      case Row(weight: Double, mu: Vector, numRows: Int, numCols: Int, sigma: String) =>
        (weight, new MultivariateGaussian(mu, Matrices.dense(numRows, numCols, sigma.split(",").map(x => x.toDouble))))
    }.unzip


    //    val dataArray = dataFrame.select("weight", "gaussians").collect()
    //
    //    val (weights, gaussians) = dataArray.map {
    //      case Row(weight: Double, gaussians:MultivariateGaussian) =>
    //        (weight, gaussians)
    //    }.unzip

    val model = new GaussianMixtureModel(weights.toArray, gaussians.toArray)
    println("----------")
    println(model.k)
    for (i <- 0 until model.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (model.weights(i), model.gaussians(i).mu, model.gaussians(i).sigma))
    }

    val data = DKUtil2.forPredictData(sc, dataType, inputPath)

    //    data.foreach(testDataLine => {
    //      val predictedClusterIndex: Int = model.predict(testDataLine)
    //
    //      println("The data " + testDataLine.toString + " belongs to cluster " +
    //        predictedClusterIndex)
    //    })
    val predictedClusterIndex = model.predict(data)

    predictedClusterIndex.zip(data).foreach(println(_))
    predictedClusterIndex.zip(data).map(x => Array(x._2, x._1).mkString("--")).saveAsTextFile(outputPath)
    sc.stop();
  }




  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    GMModelPredict.gmModelPredict(args(0),args(1),args(2),args(3))
  }



}
