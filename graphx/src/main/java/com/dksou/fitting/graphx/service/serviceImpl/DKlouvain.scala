package com.dksou.fitting.graphx.service.serviceImpl

import com.dksou.fitting.graphx.utils.{LibUtils, PathUtils, PropUtils}
import com.dksou.fitting.graphx.utils.dklouvain.HDFSLouvainRunner
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Execute the louvain distributed community detection.
 * Requires an edge file and output directory in hdfs (local files for local mode only)
 */
object DKlouvain {

  def main(args: Array[String]) {
    val inputPath = args(0)
    val sp = args(1)
    val outputPath = args(2)
    val dkmllibpath = args(3)

    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)

    conf.setAppName("louvian")
    val sc = new SparkContext(conf)

    val edgeRDD = sc.textFile(inputPath).map { line =>
      val fields = line.split(sp)
      Edge(fields(0).toLong, fields(1).toLong, 1l)
    }

    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)

    // use a helper class to execute the louvain
    // algorithm and save the output.
    // to change the outputs you can extend LouvainRunner.scala
    val minProgress = 2000
    val progressCounter = 1
    val runner = new HDFSLouvainRunner(minProgress,progressCounter,outputPath)
    runner.run(sc, graph)

    sc.stop()
  }

}



