package com.dksou.fitting.ml.service.serviceImpl.kmeans


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import com.dksou.fitting.ml.utils.MM.Cluster
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/24 0024.
 */
object KMModelBuild {

//  val log = Logger.getLogger(KMModelBuild.getClass)
  def kmModelBuild(inputPath:String,modelPath:String,numClusters:Int,dkmllibpath:String,dataType:String = "LabeledPoints"): Unit = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("RFModelBuild")

    val sc = new SparkContext(conf)


    val data = DKUtil2.forPredictData(sc, dataType, inputPath)

    val numIterations = 20
    val clusters = KMeans.train(data, numClusters, numIterations)

    val WSSSE = clusters.computeCost(data)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    println("Cluster Number:" + clusters.clusterCenters.length)

    var clusterIndex: Int = 0
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {

        println("Center Point of Cluster " + clusterIndex + ":")

        println(x)
        clusterIndex += 1
      })

    data.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)

      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })
    // Save model
    val sqlContext = new SQLContext(sc)
    val thisFormatVersion = "1.0"
    val thisClassName = "org.apache.spark.mllib.clustering.KMeansModel"

    import sqlContext.implicits._

    implicit val formats = DefaultFormats //json隐式参数

    val metadata = compact(render(("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("k" -> clusters.k)))
    sc.parallelize(Seq(metadata), 1).saveAsTextFile(new Path(modelPath, "metadata").toUri.toString)

    val dataRDD = sc.parallelize(clusters.clusterCenters.zipWithIndex, 1).map { case (point, id) =>
      Cluster(id, point)
    }.toDF()
    dataRDD.write.parquet(new Path(modelPath, "data").toUri.toString)
    sc.stop();
  }





  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    KMModelBuild.kmModelBuild(args(0),args(1),args(2).toInt,args(3))
  }




}
