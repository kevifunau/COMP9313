package comp9313.ass3

import org.apache.spark.{SparkConf, SparkContext}

object Problem1 {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    //split to get node and distance
    val preprocessedLine = input.map(line => (line.split(" ")(1).toLong,line.split(" ")(3).toDouble))
    // aggreate by keys and computer avarage distance for each node
    val AvgLength = preprocessedLine.groupByKey().map(x => (x._1, x._2.sum / x._2.size))
    // if y have same distancem, sort in decending order
    val newRdd = AvgLength.sortByKey().sortBy( y => y._2,ascending = false).map( x => x._1 + "\t" + x._2)
    // save result
    newRdd.saveAsTextFile(outputFolder)
  }

}