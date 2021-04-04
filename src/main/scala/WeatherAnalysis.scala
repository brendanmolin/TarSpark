
import java.util.{StringTokenizer, Calendar}
import java.util.logging._

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 2/25/17.
 */
object WeatherAnalysis {

  def main(args: Array[String]) {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)
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
      //val lines = ctx.textFile(logFile, 1)
      val lines = ctx.textFile("./data/all_data", 1)
      logger.info("Is lines variable empty? " + lines.isEmpty())
      logger.info("Example entry in lines: " + lines.first())
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
      }//.filter(s => WeatherAnalysis.failure(s._2))
      val output = deltaSnow.collect()
      logger.info("Testing...")
      var list = List[Long]()
      logger.info("Is output variable empty? " + output.isEmpty)
      for (o <- output) {
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