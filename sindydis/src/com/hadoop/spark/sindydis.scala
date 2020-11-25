package com.hadoop.spark

import org.apache.spark.{SparkConf, SparkContext}

object sindydis {
  def FILE_NAME : String = "/user/litianfeng/input/adult.data,/user/litianfeng/input/adult.test"
  //def FILE_NAME: String = "/user/litianfeng/input/scop.txt"
  def SAVE_PATH: String = "/user/litianfeng/output-sindy"
  def APP_NAME: String = "sindy"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME)
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    //对每个value不加区分的生成倒排索引
    val value2attr = textFile.flatMap(line => {
      val words = line.split(" ")
      var amap: Map[String, Set[Int]] = Map()
      for (i <- 0 until words.length)
        amap += (words(i) -> Set(i))
      amap
    })
    //TODO:可改为：
    // val value2attr = textFile.flatMap(line => line.split(" ").zipWithIndex)
    // + combineByKey函数

    //聚合所有相同value的索引项，生成一个set
    val value2attrs = value2attr.reduceByKey((a, b) => {
      a ++ b
    }).filter(a => a._2.size > 1)

    //对每个value对应的set，生成candidates， i -> (All - i)
    val candidate = value2attrs.flatMap(a => {
      var amap: Map[Int, Set[Int]] = Map()
      for (i <- a._2) {
        amap += (i -> (a._2 - i))
      }
      amap
    })

    //对相同index的candidate进行聚合，用集合交
    val ind = candidate.reduceByKey((a, b) => a.intersect(b)).cache()
    ind.collect().foreach(println)
    ind.saveAsTextFile(args(1))
  }
}
