package com.dksou.fitting.ml.service.serviceImpl.als

import com.dksou.fitting.ml.utils.{LibUtils, PathUtils, PropUtils, RuntimeSUtils}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
/**
 * Created by Administrator on 2016/3/25 0025.
 */
object ALSModelBuild {
//  val log = Logger.getLogger(ALSModelBuild.getClass)
private val logger = LoggerFactory.getLogger(ALSModelBuild.getClass)
   def alsModelBuild(inputPath:String,modelPath:String,rank:Int,numIterations:Int,dkmllibpath:String) = {

     val libs = LibUtils.getLibJars(dkmllibpath)
     val conf = new SparkConf()
     conf.setJars(libs)

      conf.setAppName("ALSModelBuild")
      val sc = new SparkContext(conf)


      val data = sc.textFile(inputPath).map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
      val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
      })
      val model = ALS.train(ratings, rank, numIterations, 0.01)
      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }
      val predictions =
        model.predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
     println("ALSModelBuild Mean Squared Error = " + MSE)
      val predictedAndTrue = ratesAndPreds.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
      val regressionMetrics = new RegressionMetrics(predictedAndTrue)
     println(" ALSModelBuild Mean Squared Error = " + regressionMetrics.meanSquaredError)
     println("ALSModelBuild Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)


      //    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
      //      case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
      //    }).saveAsTextFile("/tmp/result")

      model.save(sc, modelPath)
     sc.stop();
  }

  def main(args: Array[String]): Unit = {
    ALSModelBuild.alsModelBuild(args(0),args(1),args(2).toInt,args(3).toInt,args(4))
  }
}
