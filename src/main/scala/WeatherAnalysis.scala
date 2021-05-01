import collection.mutable.ArrayBuffer
import java.util.{StringTokenizer, Calendar}
import java.util.logging._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 2/25/17.
 * Modified by BDE Team on Spring 2021
 */
case class WeatherAnalysis() extends Pipeline {

  def run(): Array[((String, String), CovFloat)] = {
    try {
      // Set up spark configuration
      val sparkConf = new SparkConf()
      sparkConf.setAppName("weatherData")
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")
      // Create Spark Context
      val ctx = new SparkContext(sparkConf)
      val lines = ctx.textFile("./data/originalData/allData", 1)
      val split = lines.flatMap{s =>
        var covS = CovString(s, ArrayBuffer[Int]())
        val covtokens = covS.split(",")
        // Finds the state for a zipcode
        var state = zipToState(covtokens(0))
        var date = covtokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(covtokens(2))
        //gets year
        val year = date.substring(date.value.lastIndexOf("/") + 1)
        // gets month / date
        val monthdate = date.substring(0, date.value.lastIndexOf("/"))
        List[((String , String) , CovFloat)](
          ((state.value , monthdate.value) , snow.mergeHistory(monthdate)) ,
          ((state.value , year.value)  , snow.mergeHistory(year))
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }

      val output = deltaSnow.collect()
      println("Total number of output Records: " + output.length)
      return output
    }
  }

  def convert_to_mm(s: CovString): CovFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit.value match {
      case "mm" => return (v * 1f)
      //case "inch" => return ...
      case _ => return (v * 304.8f)
    }
  }



  def failure(record:((String, String), CovFloat)): Boolean ={
    record._2.value > 6000f
  }

  def zipToState(str : CovString):CovString = {
    // This is meant to change a zipcode to a state,
    // but currently just converts it to a number 0-49
    return (str.toInt % 50).toCovString
  }

}