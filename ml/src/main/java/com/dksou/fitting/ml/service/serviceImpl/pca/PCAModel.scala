package com.dksou.fitting.ml.service.serviceImpl.pca

import java.io.File

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.DenseMatrix
import breeze.linalg.csvwrite
import com.dksou.fitting.ml.utils.{DKUtil2, LibUtils, PathUtils, PropUtils}
/**
 * Created by Administrator on 2016/3/24 0024.
 */
object PCAModel {

//  val log = Logger.getLogger(PCAModel.getClass)
  def pcaModel(inputPath: String, outputPath: String, k: Int ,dkmllibpath:String, dataType: String = "LabeledPoints") = {


    println("dkmllibpath : " + dkmllibpath)
    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("PCACompute")

    val sc = new SparkContext(conf)

    val data = DKUtil2.forPredictData(sc, dataType, inputPath)

    val matrix = new RowMatrix(data)
    val pc = matrix.computePrincipalComponents(k)

    val col = pc.numCols
    val row = pc.numRows

    println("pc.numRows = " + row)
    println("pc.numCols = " + col)
    println("pc.toString = " + pc.toString())

    val pcm = new DenseMatrix(row, col, pc.toArray)
    csvwrite(new File(outputPath), pcm) //写出到本地，目录必须存在
    sc.stop();
  }



  def main(args: Array[String]): Unit = {
    // args(0),args(1),args(2),args(3).toInt,args(4).toInt
    PCAModel.pcaModel(args(0),args(1),args(2).toInt,args(3))
  }


}
