import collection.mutable.ArrayBuffer
import java.util.{StringTokenizer, Calendar}
import java.util.logging._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 2/25/17.
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
      val split = lines.flatMap{s =>
        var covS = CovString(s, ArrayBuffer[Int]())
        val covtokens = covS.split(",")
        // finds the state for a zipcode
        var state = zipToState(covtokens(0).value)
        var date = covtokens(1).value
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(covtokens(2).value) // CAPTURE HERE
        //lineHis.append(snowTuple._1)
        //gets year
        val year = date.substring(date.lastIndexOf("/"))
        // gets month / date
        val monthdate= date.substring(0,date.lastIndexOf("/")-1)
        List[((String , String) , Float)](
          ((state , monthdate) , snow) , // CAPTURE HERE (PROBLEM HERE: HOW TO RETURN LINE NUMBER WITH VALUE??? USE SYM PROBABLY
          ((state , year)  , snow) // CAPTURE HERE
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }//.filter(s => WeatherAnalysis.failure(s._2))
      //(key, (snow, (lines hit 1, lines hit 2)))
      val output = deltaSnow.collect()
      var list = List[Long]()
      for (o <- output.take(10)) {
        //list = o._2 :: list
        println(o)
      }
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