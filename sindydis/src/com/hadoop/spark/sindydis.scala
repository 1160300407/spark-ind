package com.hadoop.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{NewHadoopRDD}

object sindydis {
  def APP_NAME: String = "sindy"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME)
    if (args(0) == "yes")//本地模式
      conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]

    //val path :String = "file:///Users/litianfeng/PycharmProjects/lrhs"
    //val path : String = "file:///Users/litianfeng/IdeaProjects/SparkFirst/table"
    val path : String = args(1)
    val text = sc.newAPIHadoopFile(path, fc ,kc, vc, sc.hadoopConfiguration)
    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator.map(tup => (file.getPath, tup._2))
      })

    //生成倒排索引
    val value2attr = linesWithFileNames.flatMap(p =>{
      val filename = p._1.toString.substring(p._1.toString.lastIndexOf('/')+1)
      val lines = p._2.toString.split("\n")
      val result = lines.flatMap(l => {
          l.split(" ").zipWithIndex.map(
            pair => (pair._1 -> Set(filename+" "+pair._2.toString))
          )
      })
      result
    }).cache()

    //聚合所有相同value的索引项，生成一个set
    val value2attrs = value2attr.reduceByKey((a, b) => {
      a ++ b
    })

    //对每个value对应的set，生成candidates， i -> (All - i)
    val candidate = value2attrs.flatMap(a => a._2.map(s=>s->(a._2-s)))

    //对相同index的candidate进行聚合，用集合交
    val ind = candidate.reduceByKey((a, b) => a.intersect(b)).filter(x=>x._2.nonEmpty).cache()
    ind.collect().foreach(println)

    if (args.size > 2) ind.saveAsTextFile(args(2))
  }
}
