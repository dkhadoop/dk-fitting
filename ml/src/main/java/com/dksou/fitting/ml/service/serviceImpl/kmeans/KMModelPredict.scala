package com.dksou.fitting.ml.service.serviceImpl.kmeans


import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import com.dksou.fitting.ml.utils.MM.Cluster
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
/**
 * Created by Administrator on 2016/3/24 0024.
 */
object KMModelPredict {
//  val log = Logger.getLogger(KMModelPredict.getClass)

  def kmModelPredict(inputPath:String,modelPath:String,outputPath:String,dkmllibpath:String,dataType:String="LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("KMModelPredict")

    val sc = new SparkContext(conf)


    val thisFormatVersion = "1.0"
    val thisClassName = "org.apache.spark.mllib.clustering.KMeansModel"
    val sqlContext = new SQLContext(sc)

    implicit val formats = DefaultFormats

    val metadata = parse(sc.textFile(new Path(modelPath, "metadata").toUri.toString).first())
    val className = (metadata \ "class").extract[String]
    val formatVersion = (metadata \ "version").extract[String]
    val k = (metadata \ "k").extract[Int]
    assert(className == thisClassName)
    assert(formatVersion == thisFormatVersion)

    val centroids = sqlContext.parquetFile(new Path(modelPath, "data").toUri.toString)
    //Loader.checkSchema[Cluster](centroids.schema)
    val localCentroids = centroids.rdd.map(Cluster.apply).collect()
    assert(k == localCentroids.size)

    val model = new KMeansModel(localCentroids.sortBy(_.id).map(_.point))

    val data = DKUtil2.forPredictData(sc, dataType, inputPath)

    //    data.collect().foreach(testDataLine => {
    //      val predictedClusterIndex: Int = model.predict(testDataLine)
    //
    //      println("The data " + testDataLine.toString + " belongs to cluster " +
    //        predictedClusterIndex)
    //    })

    val dataAndPre = data.map(testDataLine => {
      val predictedClusterIndex: Int = model.predict(testDataLine)

      (testDataLine.toString, predictedClusterIndex)
    })
    dataAndPre.map(x => Array(x._1, x._2).mkString("--")).saveAsTextFile(outputPath)
    sc.stop();
  }





  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    KMModelPredict.kmModelPredict(args(0),args(1),args(2),args(3))
  }

}
