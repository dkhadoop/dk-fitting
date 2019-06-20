package com.dksou.fitting.ml.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._



/**
 * Created by Administrator on 2016/3/23 0023.
 */
object DKUtil2 {
  def getNumFeatures(sc: SparkContext,modelPath: String):Int={
    implicit val formats = DefaultFormats//json隐式参数
    val metadata = parse(sc.textFile(new Path(modelPath, "metadata").toUri.toString).first())

    val numFeatures = (metadata \ "numFeatures").extract[String].toInt
    numFeatures
  }


  def forBuildData(sc: SparkContext, dataType: String, inputPath: String): RDD[LabeledPoint] = {
    var data: RDD[LabeledPoint] = null
    if (dataType == "LibSVM") {
      data = MLUtils.loadLibSVMFile(sc, inputPath)
    }
    else if (dataType == "LabeledPoints") {
      val text = sc.textFile(inputPath).map(_.trim).filter(line => !(line.isEmpty || line.startsWith("#")))
      data = text.map { line =>
        val parts = line.split(",")
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(java.lang.Double.parseDouble)))
      }
    }
    data
  }


  def forPredictData(sc: SparkContext, dataType: String, inputPath: String): RDD[Vector] = {
    var data: RDD[Vector] = null

    if (dataType == "LibSVM") {
      val parsed = sc.textFile(inputPath)
        .map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
        .map { line =>
          val items = line.split(' ')
          val (indices, values) = items.filter(_.nonEmpty).map { item =>
            val indexAndValue = item.split(':')
            val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
          val value = indexAndValue(1).toDouble
            (index, value)
          }.unzip
          (indices.toArray, values.toArray)
        }

      // Determine number of features.
      val d = {
        parsed.map { case (indices, values) =>
          indices.lastOption.getOrElse(0)
        }.reduce(math.max) + 1
      }

      data = parsed.map { case (indices, values) =>
        Vectors.sparse(d, indices, values)
      }

    }


    else if (dataType == "LabeledPoints") {
      val text = sc.textFile(inputPath).map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
      data = text.map { line =>
        val parts = line.split(",")
        Vectors.dense(parts.map(java.lang.Double.parseDouble))
      }
    }
    data
  }

  def forPredictData(sc: SparkContext, dataType: String, numFeatures: Int, inputPath: String): RDD[Vector] = {
    var data: RDD[Vector] = null

    if (dataType == "LibSVM") {
      val parsed = sc.textFile(inputPath)
        .map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
        .map { line =>
          val items = line.split(' ')
          val (indices, values) = items.filter(_.nonEmpty).map { item =>
            val indexAndValue = item.split(':')
            val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
          val value = indexAndValue(1).toDouble
            (index, value)
          }.unzip
          (indices.toArray, values.toArray)
        }

      // Determine number of features.
      val d = numFeatures

      data = parsed.map { case (indices, values) =>
        Vectors.sparse(d, indices, values)
      }
    }


    else if (dataType == "LabeledPoints") {
      val text = sc.textFile(inputPath).map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
      data = text.map { line =>
        val parts = line.split(",")
        Vectors.dense(parts.map(java.lang.Double.parseDouble))
      }
    }
    data
  }

}
