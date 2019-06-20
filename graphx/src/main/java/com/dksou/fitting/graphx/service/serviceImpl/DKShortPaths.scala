package com.dksou.fitting.graphx.service.serviceImpl

import com.dksou.fitting.graphx.utils.{LibUtils, PathUtils, PropUtils}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/8/3 0003.
 */
object DKShortPaths {
  def main(args: Array[String]) {
    //数据必须为数值型
    val inputPath = args(0)
    val sp = args(1)
    val outputPath = args(2)

    val dkmllibpath = args(3)

    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)


      conf.setAppName("DKShortPaths")
    val sc = new SparkContext(conf)

    val links: RDD[String] = sc.textFile(inputPath)

    val edges = links.map { line =>
      val fields = line.split(sp)
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }

    val graph = Graph.fromEdges(edges, 0, StorageLevel.MEMORY_AND_DISK_SER, StorageLevel.MEMORY_AND_DISK_SER).partitionBy(EdgePartition2D)
    val landmarks = graph.vertices.map(v=>v._1).collect()
    val result = ShortestPaths.run(graph, landmarks)

    result.vertices.mapValues(_.filter(_._2==2).keys.mkString("|")).saveAsTextFile(outputPath)
    sc.stop()
  }
}
