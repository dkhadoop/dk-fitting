package com.dksou.fitting.ml.service.serviceImpl.fpgrowth

import com.dksou.fitting.ml.utils.{LibUtils, PathUtils, PropUtils}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2016/3/25 0025.
 */
object FPGrowthModel {

//  val log = Logger.getLogger(FPGrowthModel.getClass)
  def fpGrowthModelBuild(inputPath:String,outputPath:String,minSupport:Double,dkmllibpath:String) = {

    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("FPGrowthModelBuild").set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)



    val data = sc.textFile(inputPath).map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#"))).map(_.split(","))

    val model = new FPGrowth().setMinSupport(minSupport).run(data)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ": " + itemset.freq)
    }
    //model.freqItemsets.saveAsTextFile(outputPath)
    model.freqItemsets
      .map { itemset => (itemset.items.mkString("[", ",", "]"), itemset.freq) }
      .sortBy(_._2, false, 1).map(x => Array(x._1, x._2).mkString(": ")).saveAsTextFile(outputPath)
    sc.stop();
  }


  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    FPGrowthModel.fpGrowthModelBuild(args(0),args(1),args(2).toDouble,args(3))
  }

}
