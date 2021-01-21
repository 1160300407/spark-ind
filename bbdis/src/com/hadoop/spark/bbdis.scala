package com.hadoop.spark
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD

import scala.Array.concat

object bbdis {
  def APP_NAME: String = "b&b"
  def typeList: Array[String] = Array("string", "int")

  //line中所有string全为数字，则返回true
  def isTypeInt(line: Seq[String]): Boolean = {
    val regex = """^\d+$""".r
    return line.forall(s => regex.findFirstMatchIn(s) != None)
  }

  //计算line <= base[?], 返回一个数组，如[1,2,3],表示第一列包含依赖于第二列和第三列。
  def calculateInd(line: (String, List[String]), base: Array[(String, List[String])]) = {
    val rhss = base.filter(x => {
      val rhs = x._2.toSet
      (line._1 != x._1).&&(line._2.forall(s => rhs.contains(s)))
    }).map(x => x._1)
    rhss
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME)
    if (args(0) == "yes") //本地模式
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

    val value2attr = linesWithFileNames.flatMap(p =>{
      val filename = p._1.toString.substring(p._1.toString.lastIndexOf('/')+1)
      val lines = p._2.toString.split("\n")
      val result = lines.flatMap(l => {
        l.split(" ").zipWithIndex.map(
          pair => (pair._1 -> (filename+"-"+pair._2.toString))
        )
      })
      result
    }).cache()

    val t = value2attr.groupBy(_._2)
      .mapValues(_.map(_._1))
      .collectAsMap()
      .map(s => (s._1,s._2.toList))
      .toSeq
      .map(s => (s._1, s._2.min +: s._2.max +: s._2))//加入最小和最大值，加速后续的包含判断
    val columnsData = sc.parallelize(t)//转置并生成RDD

    //用typeIntData存储所有数字属性数据 NoInt存储字符
    val typeIntData = columnsData.filter(line => isTypeInt(line._2) == true)
    val typeNoIntData = columnsData.filter(line => isTypeInt(line._2) == false)
    //将两类数据全部发送到所有节点
    val baseIntData = sc.broadcast(typeIntData.collect)
    val baseNoIntData = sc.broadcast(typeNoIntData.collect)

    //两类数据分别计算ind
    val ind = typeNoIntData.map(line =>
      line._1->calculateInd(line, baseNoIntData.value)
    ).cache()

    val ind2 = typeIntData.map(line =>
      line._1->calculateInd(line, baseIntData.value)
    ).cache()

    ind.collect()
      .filter(x => x._2.length != 0)
      .foreach(x=>println(x._1+","+x._2.toSet.toString()))
    ind2.collect()
      .filter(x => x._2.length != 0)
      .foreach(x=>println(x._1+","+x._2.toSet.toString()))

    if (args.size > 2) {
      ind.saveAsTextFile(args(2)+"-String")
      ind2.saveAsTextFile(args(2)+"-Int")
    }
  }
}
