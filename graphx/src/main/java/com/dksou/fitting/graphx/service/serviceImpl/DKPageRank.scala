package com.dksou.fitting.graphx.service.serviceImpl

import com.dksou.fitting.graphx.utils.{GuavaUtil, LibUtils, PathUtils, PropUtils}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/8/1 0001.
 */
object DKPageRank {
  def main(args: Array[String]) {
//数据可以为字符串或者数值
    val inputPath = args(0)
    val sp = args(1)
    val tp = args(2)
    val outputPath = args(3)

    val dkmllibpath = args(4)

    val libs = LibUtils.getLibJars(dkmllibpath)
    val conf = new SparkConf()
    conf.setJars(libs)


      conf.setAppName("PageRank")
    val sc = new SparkContext(conf)

    //读入数据文件，只需边数据，此处为原始字符串数据
    val links = sc.textFile(inputPath)

    if (tp.equalsIgnoreCase("str")){
      //边数据数值化
      val edges = links.map { line =>
        val fields = line.split(sp)
        Edge(GuavaUtil.hashId(fields(0)), GuavaUtil.hashId(fields(1)), 0)
      }

      //对边数据进行去重，产生顶点及其属性数据
      val vertices = links.flatMap(line => line.split(sp)).distinct().map(str=>(GuavaUtil.hashId(str),str))

      //构建图
      val graph = Graph(vertices, edges, "").persist()

      //运行
      val prGraph = graph.pageRank(0.001).cache()


      val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
        //顶点id，graph顶点属性，prGraph顶点属性 => 修改完成后的属性
        (v, title, rank) => (rank.getOrElse(0.0), title)
      }
      

      //    titleAndPrGraph.vertices.top(10) {
      //      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
      //    }.foreach(t => println(t._2._2 + ": " + t._2._1))

      titleAndPrGraph.vertices.sortBy((entry: (VertexId, (Double, String))) => entry._2._1,false)
        .map(t => t._2._2 + ": " + t._2._1).saveAsTextFile(outputPath)
    }
    else if(tp.equalsIgnoreCase("num")){
      val edges = links.map { line =>
        val fields = line.split(sp)
        Edge(fields(0).toLong, fields(1).toLong, 0)
      }

      val graph = Graph.fromEdges(edges,0,StorageLevel.MEMORY_AND_DISK_SER,StorageLevel.MEMORY_AND_DISK_SER).partitionBy(EdgePartition2D)

      val prGraph = graph.pageRank(0.001).cache()

      val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
        //顶点id，graph顶点属性，prGraph顶点属性 => 修改完成后的属性
        (v, nul, rank) => (rank.getOrElse(0.0))
      }

      titleAndPrGraph.vertices.sortBy((entry: (VertexId, Double)) => entry._2,false)
        .map(t => t._1 + ": " + t._2).saveAsTextFile(outputPath)
    }

    sc.stop()
  }
}
