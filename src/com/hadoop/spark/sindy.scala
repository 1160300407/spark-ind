package com.hadoop.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sindy {
  def FILE_NAME : String = "file:///Users/litianfeng/Documents/scop.txt"
  def APP_NAME : String = "sindy"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val sc = new SparkContext(conf)
    //single input file
    val textFile = sc.textFile(FILE_NAME)
    //multi input file TODO
  //textFile.
    val value2attr = textFile.flatMap(line => {
      val words = line.split(" ")
      var amap: Map[String, Set[Int]] = Map()
      for (i <- 0 until words.length)
        amap += (words(i) -> Set(i))
      amap
    })

    val value2attrs = value2attr.reduceByKey((a, b) => {
      a ++ b
    }).filter(a => a._2.size > 1)

    //val v2a = value2attrs.collect()
    //println(v2a.foreach(println))
    val candidate = value2attrs.flatMap(a => {
        var amap: Map[Int, Set[Int]] = Map()
        for (i <- a._2) {
          amap += (i -> (a._2 - i))
        }
        amap
    })

    val ind = candidate.reduceByKey((a, b) => a.intersect(b))

    println("ind result:")
    ind.collect().foreach(println)
    ind.saveAsTextFile("/Users/litianfeng/Documents/ind")
  }
}

/**
 * //选取特定列.
 * val data=sc.textFile("file:///E://table//wordcount.txt")
 * .flatMap(_.split("\n"))  //按换行符分割文件，把文件分成一行行的
 * .map{
 * line=>
 * var splits=line.split(",").reverse(0)   //把行按","分割，转置选取第一列即最后一列，
 * //选取其他列例如第一列：line.split(",")(0) ，
 * //选取多列map(line=>(line.split(",")(0),line.split(",")(18),line.split(",")(31)))
 * (splits,1)
 * }.reduceByKey(_+_).collect().foreach(println)
 */