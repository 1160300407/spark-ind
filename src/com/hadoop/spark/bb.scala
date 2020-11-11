package com.hadoop.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import Array._

object bb {
  def FILE_NAME : String = "file:///Users/litianfeng/Documents/scop.txt"
  def APP_NAME : String = "bitset"
  def typeList : Array[String] = Array("string", "int", "double")
  def isTypeInt(line: Seq[String]): Boolean = {
    val regex="""^\d+$""".r
    return line.forall(s => regex.findFirstMatchIn(s) != None)
    //return regex.findAllMatchIn(line).toList.size
  }
  def calculateInd(line: (Seq[String], Int), base: Array[(Seq[String], Int)])  = {
    //println(line._2)
    val lhs = Array(line._2)
    concat(lhs, base.filter(x => {
      val rhs = x._1.toSet
      line._1.forall(s => rhs.contains(s))
    }).map(x => x._2))
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val sc = new SparkContext(conf)
    //single input file
    val textFile = sc.textFile(FILE_NAME)
    //multi input file TODO

    val seperateTF = textFile.map(line=>line.split(" ")).collect().toSeq
    val columnsData = sc.parallelize({
      val temp = seperateTF.transpose
      temp.foreach(x =>
        x.min +: x.max +: x
      )
      temp.zipWithIndex
    })

    val typeIntData = columnsData.filter(line => isTypeInt(line._1) == true)
    val typeNoIntData = columnsData.filter(line => isTypeInt(line._1) == false)
                                  //  .map(line=>(line._1.sorted,line._2)) //sort!
    //typeNoIntData.collect.foreach((a) => println(a._1(0),a._2))
    //typeIntData.max()
    val newtypeIntData = typeIntData.map(a => {

    })
    val baseIntData = sc.broadcast(typeIntData.collect)
    val baseNoIntData = sc.broadcast(typeNoIntData.collect)


    val ind = typeNoIntData.map(line =>
      calculateInd(line, baseNoIntData.value)
    )
    val ind2 = typeIntData.map(line =>
      calculateInd(line, baseNoIntData.value)
    )
    println("ind calc over!")
    ind.collect().foreach(a => println(a.mkString(",")))
    //println(baseNoIntData.value(0)._2)
    //println(typeNoIntData.collect.toSeq(0)._2)

    //val pp = typeNoIntData.collect.toSeq(0)._1.toSet
    //val qq = baseNoIntData.value(0)._1
    //println(qq.forall(s=>pp.contains(s)))
  }
}