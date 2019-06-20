package com.dksou.fitting.ml.service.serviceImpl.gaussian

import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
import com.dksou.fitting.ml.utils.MM.{Data, Gau}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/24 0024.
 */
object GMModelBuild {

//  val log = Logger.getLogger(GMModelBuild.getClass)
  def gmModelBuild(inputPath:String,modelPath:String,numClusters:Int,dkmllibpath:String,dataType:String = "LabeledPoints") = {

    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("GMModelBuild")

    val sc = new SparkContext(conf)

    val data = DKUtil2.forPredictData(sc, dataType, inputPath)
    val gmm = new GaussianMixture().setK(numClusters).run(data)

    // output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }


    // Save and load model
    //    gmm.save(sc, modelPath)
    //val sameModel = GaussianMixtureModel.load(sc, modelPath)
    val sqlContext = new SQLContext(sc)
    val formatVersionV1_0 = "1.0"
    val classNameV1_0 = "org.apache.spark.mllib.clustering.GaussianMixtureModel"

    import sqlContext.implicits._
    // Create JSON metadata.
    val metadata = compact(render
    (("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~ ("k" -> gmm.weights.length)))
    sc.parallelize(Seq(metadata), 1).saveAsTextFile(new Path(modelPath, "metadata").toUri.toString)

    // Create Parquet data.
    val dataArray = Array.tabulate(gmm.weights.length) { i =>
      Data(gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma.numRows, gmm.gaussians(i).sigma.numCols, gmm.gaussians(i).sigma.toArray.mkString(","))
    }

    //    val dataArray = Array.tabulate(gmm.weights.length) { i =>
    //      Gau(gmm.weights(i), gmm.gaussians(i))
    //    }
    val dataRDD = sc.parallelize(dataArray, 1).toDF()
    dataRDD.write.parquet(new Path(modelPath, "data").toUri.toString)
    sc.stop();
  }


  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    GMModelBuild.gmModelBuild(args(0),args(1),args(2).toInt,args(3))
  }

}
