package comp9313.ass3

import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD

object Problem2 {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val SourceID = args(1).toLong
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)

    val v2vDistRDD = input.map(line=>line.split(" "))

    // build graph by make vertexRDD and edgeRDD
    val vertexArrayRDD = (v2vDistRDD.map(x=>x(1)) ++ v2vDistRDD.map(x=>x(2))).distinct.map(x=> (x.toLong,Double.PositiveInfinity))
    val edgeArrayRDD = v2vDistRDD.map(x=>Edge(x(1).toInt,x(2).toInt,1.0))

    val graph = Graph(vertexArrayRDD,edgeArrayRDD)

    // start with all vertices with infinity distance except the source node
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == SourceID) 0.0 else Double.PositiveInfinity)

    // Pregel Dijkstra  to find the distance from source to each node
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(

      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    // remove source itself and unreached node
    println(sssp.vertices.filter(x => x._2 != Double.PositiveInfinity && x._1 != SourceID ).count())

  }













}
