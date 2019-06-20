package com.dksou.fitting.ml.service.serviceImpl.als

import com.dksou.fitting.ml.utils.{LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import scala.collection.mutable

/**
 * Created by Administrator on 2016/3/25 0025.
 */
object RMUsers {
//  val log = Logger.getLogger(RMUsers.getClass)

  def rmUsers(inputPath:String,modelPath:String,outputPath:String,dkmllibpath:String) = {

    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("ALSRMUsers")

    val sc = new SparkContext(conf)


    val model = MatrixFactorizationModel.load(sc, modelPath)

    val data = sc.textFile(inputPath).distinct().map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#"))).collect()

    val rawData = new mutable.HashMap[String, String]()
    data.foreach(
      product => {
        // 依次为商品推荐用户
        val rs = model.recommendUsers(product.toInt, 10)
        var value = ""
        var key = 0
        // 拼接推荐结果
        rs.foreach(r => {
          key = r.product
          value = value + r.user + ":" + r.rating + ","
        })
        println("商品--推荐的用户：分值, ······")
        println(key + "--" + value)
        rawData.put(key.toString, value.substring(0, value.length - 1))
      }
    )

    val rs = sc.parallelize(rawData.toSeq).map(x => Array(x._1, x._2).mkString("--"))
    rs.saveAsTextFile(outputPath)
    sc.stop();
  }


  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    RMUsers.rmUsers(args(0),args(1),args(2),args(3))
  }
}
