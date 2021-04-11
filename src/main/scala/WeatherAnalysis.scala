import org.apache.spark.rdd.RDD

import collection.mutable.ArrayBuffer
import java.util.{Calendar, StringTokenizer}
import java.util.logging._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ali on 2/25/17.
 * Modified by BDE Team on Spring 2021
 */
object WeatherAnalysis {

  def main(args: Array[String]) {
    try {
      //set up spark configuration
      val sparkConf = new SparkConf()

      var logFile = ""
      var local = 500
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/data/logfiles"
      } else {

        logFile = args(0)
        local = args(1).toInt

      }
      val ctx = new SparkContext(sparkConf)
      val lines = ctx.textFile("./data/all_data", 1)
      for (l <- lines.take(40)) {
        //list = o._2 :: list
        println(l)
      }

      val lines_split = lines.flatMap{s =>
        var covS = CovString(s, ArrayBuffer[Int]())
        val covtokens = covS.split(",")
        // finds the state for a zipcode
        var state = zipToState(covtokens(0))
        var date = covtokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(covtokens(2)) // CAPTURE HERE
        //gets year
        val year = date.value.substring(date.value.lastIndexOf("/"))
        // gets month / date
        val monthdate= date.value.substring(0,date.value.lastIndexOf("/"))
        List[((String , String) , CovFloat)](
          ((state.value , monthdate) , snow.appendHistory(44)) , // CAPTURE HERE (PROBLEM HERE: HOW TO RETURN LINE NUMBER WITH VALUE??? USE SYM PROBABLY
          ((state.value , year)  , snow.appendHistory(45)) // CAPTURE HERE
        ).iterator
      }
      for (sv <- lines_split.take(40)) {
        //list = o._2 :: list
        println(sv)
      }
      val deltaSnow = lines_split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }//.filter(s => WeatherAnalysis.failure(s._2))
      val output = deltaSnow.collect()
      var list = List[Long]()
      for (o <- output.take(30)) {
        //list = o._2 :: list
        println(o)
      }
    }
  }

  def convert_to_mm(s: CovString): CovFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit.value match {
      case "mm" => return CovFloat(v.value, v.hist+=68)
      case _ => return v * (304.8f, 69)
    }
  }
  def failure(record:Float): Boolean ={
    record > 6000f
  }
  def zipToState(str : CovString):CovString = {
    // This is meant to change a zipcode to a state,
    // but currently just converts it to a number 0-49
    return (str.toInt % 50).toCovString
  }

}