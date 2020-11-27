package com.hadoop.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.Array.concat

object bbdis {
  def APP_NAME: String = "b&b"
  def typeList: Array[String] = Array("string", "int")
  def MASTER_NAME: String = "local[*]"
  //line中所有string全为数字，则返回true
  def isTypeInt(line: Seq[String]): Boolean = {
    val regex = """^\d+$""".r
    return line.forall(s => regex.findFirstMatchIn(s) != None)
  }
  //计算line <= base[?], 返回一个数组，如[1,2,3],表示第一列包含依赖于第二列和第三列。
  def calculateInd(line: (Array[String], String), base: Array[(Array[String], String)]) = {
    val lhs = Array(line._2)
    val rhss = base.filter(x => {
      val rhs = x._1.toSet
      (line._2 != x._2).&&(line._1.forall(s => rhs.contains(s)))
    }).map(x => x._2)
    concat(lhs, rhss)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME)
    if (args(0) == "local")
      conf.setMaster(MASTER_NAME)
    val sc = new SparkContext(conf)

    val textFile = sc.wholeTextFiles(args(1))
    val columnsData = textFile.flatMap(file => {
      val filename = file._1.substring(file._1.lastIndexOf('/')+1, file._1.lastIndexOf('.'))
      //将行转置为列， 并在新的行前面加入该行的最小值和最大值，加速后续的包含判断
      val sepe = file._2.split("\n").map(line => line.split(" ")).transpose
      sepe.foreach(x => x.min +: x.max +: x)
      sepe.zipWithIndex.map(pair => (pair._1 -> (filename+"-"+pair._2.toString)))
    })

    //用typeIntData存储所有数字属性数据 NoInt存储字符
    val typeIntData = columnsData.filter(line => isTypeInt(line._1) == true)
    val typeNoIntData = columnsData.filter(line => isTypeInt(line._1) == false)

    //将两类数据全部发送到所有节点
    val baseIntData = sc.broadcast(typeIntData.collect)
    val baseNoIntData = sc.broadcast(typeNoIntData.collect)

    //两类数据分别计算ind
    val ind = typeNoIntData.map(line =>
      calculateInd(line, baseNoIntData.value)
    )
    val ind2 = typeIntData.map(line =>
      calculateInd(line, baseIntData.value)
    )

    println("ind calc over!")
    ind.collect()
      .filter(x => x.length > 1)
      .foreach(x=> {println(x(0)+":"+x.drop(1).mkString(","))})
    ind2.collect()
      .filter(x => x.length > 1)
      .foreach(x=>{println(x(0)+":"+x.drop(1).mkString(","))})

    if (args.size > 2) {
      ind.saveAsTextFile(args(1) + "-type1")
      ind2.saveAsTextFile(args(1) + "-type2")
    }
  }
}
