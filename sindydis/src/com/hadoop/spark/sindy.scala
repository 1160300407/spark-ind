package com.hadoop.spark

import org.apache.spark.{SparkConf, SparkContext}

object sindy {
  def APP_NAME: String = "sindy"
  def MASTER_NAME: String = "local[*]"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER_NAME)
    val sc = new SparkContext(conf)
    //对每个value不加区分的生成倒排索引
    val textFile = sc.wholeTextFiles(args(0))
    val value2attr = textFile.flatMap(file => {
      val filename = file._1.substring(file._1.lastIndexOf('/')+1, file._1.lastIndexOf('.'))
      file._2.split("\n").flatMap(line => {
        line.split(" ").zipWithIndex.map(pair => (pair._1 -> Set(filename+"-"+pair._2.toString)))
      })
    })

    //聚合所有相同value的索引项，生成一个set
    val value2attrs = value2attr.reduceByKey((a, b) => {
      a ++ b
    }).filter(a => a._2.size > 1)

    //对每个value对应的set，生成candidates， i -> (All - i)
    val candidate = value2attrs.flatMap(a => {
      var amap: Map[String, Set[String]] = Map()
      for (i <- a._2) {
        amap += (i -> (a._2 - i))
      }
      amap
    })

    //对相同index的candidate进行聚合，用集合交
    val ind = candidate.reduceByKey((a, b) => a.intersect(b))
    ind.collect().foreach(println)
    //ind.saveAsTextFile(SAVE_PATH)
  }
}
