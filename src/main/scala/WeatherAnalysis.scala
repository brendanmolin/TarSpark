import collection.mutable.ArrayBuffer
import java.util.{Calendar, StringTokenizer}
import java.util.logging._
import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * Created by ali on 2/25/17.
 * Modified by BDE Team on Spring 2021
 */
object WeatherAnalysis {

  def main(args: Array[String]) {
    try {
      //set up spark configuration
      val sparkConf = new SparkConf()
      sparkConf.setAppName("weatherData")
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")

      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")

      val ctx = new SparkContext(sparkConf)
      val lines = ctx.textFile("./data/all_data", 1)
      println("Is lines variable empty? " + lines.isEmpty())
      println("Example entry in lines: " + lines.first())
      val split = lines.flatMap{s =>
        var covS = CovString(s, ArrayBuffer[Int]())
        val covtokens = covS.split(",")
        // finds the state for a zipcode
        var state = zipToState(covtokens(0))
        var date = covtokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(covtokens(2)) // CAPTURE HERE
        //gets year
        val year = date.value.substring(date.value.lastIndexOf("/") + 1)
        // gets month / date
        val monthdate= date.value.substring(0, date.value.lastIndexOf("/"))
        List[((String , String) , CovFloat)](
          ((state.value , monthdate) , snow.appendHistory(44)) , // CAPTURE HERE (PROBLEM HERE: HOW TO RETURN LINE NUMBER WITH VALUE??? USE SYM PROBABLY
          ((state.value , year)  , snow.appendHistory(45)) // CAPTURE HERE
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }//.filter(s => WeatherAnalysis.failure(s._2))
      val output = deltaSnow.collect()

      for (o <- output.take(10)) {
        println(o)
      }


      //TODO add function to test output and return dictionary of line numbers and failure %
    }
  }

  def convert_to_mm(s: CovString): CovFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit.value match {
      case "mm" => return CovFloat(v.value, v.hist += 68)
      case _ => return CovFloat(v.value * 304.8f, v.hist += 69)
    }
  }

  def detectFailedLines(resultList:List[((String, String), CovFloat)]): collection.mutable.HashMap[Int, (Int, Int)] = {
    // (lineNo, (No of passing records, No of failing records)
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)]() // Create new empty Map
    for (o <- resultList){
      val isFailure = failure(o._2.value)
      for (eaLine <- o._2.hist){
        if (!resultMap.contains(eaLine)){ // If this line No isn't in the dictionary yet, add it
          resultMap.+=((eaLine, (0, 0)))
        }
        case isFailure => resultMap.update(eaLine, resultMap.get(eaLine))
          // If the entry was a failure, update key eaLine (x, y) -> (x, y+1)
        case _ => //update key eaLine (x, y) => (x+1, y)
      }
    }
    return resultMap
  }

  def failure(record:Float): Boolean ={
    record > 6000f
  }
  def zipToState(str : CovString):CovString = {
    // This is meant to change a zipcode to a state,
    // but currently just converts it to a number 0-49
    // TODO fix this potentially?
    return (str.toInt % 50).toCovString
  }

}