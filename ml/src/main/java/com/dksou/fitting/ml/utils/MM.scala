package com.dksou.fitting.ml.utils

import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Matrices, Matrix}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.{MLUtils, Loader, Saveable}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.sql.{SQLContext, Row}

object MM {

  case class Cluster(id: Int, point: Vector)

  object Cluster {
    def apply(r: Row): Cluster = {
      Cluster(r.getInt(0), r.getAs[Vector](1))
    }
  }

  //case class Data(weight: Double, mu: Vector, sigma: Matrix)
  //case class Data(weight: Double, mu: Vector, numRows:Int,numCols:Int,sigma: Array[Double])
  case class Data(weight: Double, mu: Vector, numRows:Int,numCols:Int,sigma: String)
  case class Gau(weight: Double, gaussians:MultivariateGaussian)
}