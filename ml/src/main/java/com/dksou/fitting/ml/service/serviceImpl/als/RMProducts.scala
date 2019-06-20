package com.dksou.fitting.ml.service.serviceImpl.als

import com.dksou.fitting.ml.utils.{LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import scala.collection.mutable

/**
 * Created by Administrator on 2016/3/25 0025.
 */
object RMProducts {
//  val log = Logger.getLogger(RMProducts.getClass)

  def rmpProducts(inputPath:String,modelPath:String,outputPath:String,dkmllibpath:String) = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("RMProducts")

    val sc = new SparkContext(conf)


    val model = MatrixFactorizationModel.load(sc, modelPath)

    val data = sc.textFile(inputPath).distinct().map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#"))).collect()

    val rawData = new mutable.HashMap[String, String]()
    data.foreach(
      user => {
        // 依次为用户推荐商品
        val rs = model.recommendProducts(user.toInt, 10)
        var value = ""
        var key = 0
        // 拼接推荐结果
        rs.foreach(r => {
          key = r.user
          value = value + r.product + ":" + r.rating + ","
        })
        println("用户--推荐的商品：分值, ······")
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
    RMProducts.rmpProducts(args(0),args(1),args(2),args(3))
  }
}
