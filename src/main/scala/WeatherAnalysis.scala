
import java.util.{StringTokenizer, Calendar}
import java.util.logging._

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 2/25/17.
 */
object WeatherAnalysis {

  def main(args: Array[String]) {
      //set up spark configuration
      val sparkConf = new SparkConf()
      sparkConf.setAppName("weatherData")
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")

      val ctx = new SparkContext(sparkConf)
      //val lines = ctx.textFile(logFile, 1)
      val lines = ctx.textFile("./data/all_data", 1)
      println("Is lines variable empty? " + lines.isEmpty())
      println("Example entry in lines: " + lines.first())
      val split = lines.flatMap{s =>
        val tokens = s.split(",")
        // finds the state for a zipcode
        var state = zipToState(tokens(0))
        var date = tokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(tokens(2))
        //gets year
        val year = date.substring(date.lastIndexOf("/"))
        // gets month / date
        val monthdate= date.substring(0,date.lastIndexOf("/")-1)
        List[((String , String) , Float)](
          ((state , monthdate) , snow) ,
          ((state , year)  , snow)
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val delta =  s._2.max - s._2.min
        (s._1 , delta)
      }
      val failedOutputs = deltaSnow.filter(s => WeatherAnalysis.failure(s._2))
      println("deltaSnow take 10:")
      for (o <- deltaSnow.take(10)) {
        println(o)
      }
      println("Failures:")
      for (o <- failedOutputs.take(10)) {
        println(o)
      }
    while (true) {
      // Create infinite while loop to keep localhost:4040 up and running
    }
  }

  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }
  def failure(record:Float): Boolean ={
    record > 6000f
  }
  def zipToState(str : String):String = {
    // This is meant to change a zipcode to a state,
    // but currently just converts it to a number 0-49
    return (str.toInt % 50).toString
  }

}