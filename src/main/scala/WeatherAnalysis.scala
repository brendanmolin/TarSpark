import collection.mutable.ArrayBuffer
import java.util.{StringTokenizer, Calendar}
import java.util.logging._
import org.apache.spark.{SparkContext, SparkConf}

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
        val year = date.diverge().substring(date.value.lastIndexOf("/") + 1).appendHistory(36)
        // gets month / date
        val monthdate= date.diverge().substring(0, date.value.lastIndexOf("/")).appendHistory(38)

        List[((String , String) , CovFloat)](
          ((state.value , monthdate.value) , snow.diverge().mergeHistory(monthdate)) ,
          ((state.value , year.value)  , snow.diverge().mergeHistory(year))
        ).iterator
      }
      for (sn <- split.take(10)) {
        println(sn)
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }//.filter(s => WeatherAnalysis.failure(s._2))

      val output = deltaSnow.collect()
      var list = List[Long]()
      for (o <- output.take(10)) {
        println(o)
      }
      println(detectFailedLines(output))
    }
  }

  def convert_to_mm(s: CovString): CovFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit.value match {
      case "mm" => return v.appendHistory(68)
      case _ => return (v * 304.8f).appendHistory(69)
    }
  }

  def detectFailedLines(resultList:Array[((String, String), CovFloat)]): collection.mutable.HashMap[Int, (Int, Int)] = {
    // (lineNo, (No of passing records, No of failing records)
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)]() // Create new empty Map
    for (o <- resultList){
      val isFailure = failure(o._2.value)
      for (eaLine <- o._2.hist){
        if (!resultMap.contains(eaLine)){ // If this line No isn't in the dictionary yet, add it
          resultMap.+=((eaLine, (0, 0)))
        }
        if (isFailure){
          val rec = resultMap.get(eaLine)
          val new_rec = (rec.get._1, 1 + rec.get._2)
          resultMap.update(eaLine, new_rec) // tuple can be stored in a separate var if error returned
        } else {
          val rec = resultMap.get(eaLine)
          val new_rec = (1 + rec.get._1, rec.get._2)
          resultMap.update(eaLine, new_rec) // tuple can be stored in a separate var if error returned
        }
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