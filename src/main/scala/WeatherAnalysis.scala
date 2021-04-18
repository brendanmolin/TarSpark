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

        val year = date.divergeStrCopy(date).dateSubstring(date.value.lastIndexOf("/") + 1, 36)
        // gets month / date
        val monthdate= date.divergeStrCopy(date).dateSubstring(0, date.value.lastIndexOf("/"), 38)

        val snowMonthdate = snow.divergeFloatCopy(snow)
        val snowYear = snow.divergeFloatCopy(snow)


        List[((String , String) , CovFloat)](
          ((state.value , monthdate.value) , snowMonthdate.mergeHistory(snowMonthdate, monthdate).distinctHist(snowMonthdate)) , // CAPTURE HERE (PROBLEM HERE: HOW TO RETURN LINE NUMBER WITH VALUE??? USE SYM PROBABLY
          ((state.value , year.value)  , snowYear.mergeHistory(snowYear, year).distinctHist(snowYear) ) //CAPTURE HERE
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
      val output2 = summarizeFailLines(output)
      val resultMap = output2._1
      val totalNumberOfPasses = output2._2
      val totalNumberOfFailures = output2._3

      println(resultMap)
      println(totalNumberOfPasses)
      println(totalNumberOfFailures)
      getFailedLine(resultMap, totalNumberOfPasses, totalNumberOfFailures)
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

  def summarizeFailLines(resultList:Array[((String, String), CovFloat)]): (collection.mutable.HashMap[Int, (Int, Int)], Int, Int) = {
    // (lineNo, (No of passing records, No of failing records)
    var totalNumberOfPasses = resultList.length
    var totalNumberOfFailures = 0
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)]() // Create new empty Map
    for (o <- resultList){
      val isFailure = failure(o._2.value)
      if (isFailure){
        totalNumberOfFailures += 1
        totalNumberOfPasses -= 1
      }
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
    return (resultMap, totalNumberOfPasses, totalNumberOfFailures)
  }

  def getFailedLine(resultMap:collection.mutable.HashMap[Int, (Int, Int)], totalNumberOfPasses:Int, totalNumberOfFailures:Int): Int = {
    /* Takes the result map of (lineNo, (passing cases, failing cases))
        and returns the line number most likely to be causing the issue.
        Returns -1 if no failures were detected.
     */
    if (totalNumberOfFailures == 0){
      return -1 /* No failing lines */
    }
    //val LineRankings = collection.mutable.HashMap[Int, Int]() // Create new empty Map
    var lineRankings = ArrayBuffer[(Int, Double)]() //Create new list of Tuples
    for (eaLine <- resultMap){
      val lineNo = eaLine._1
      val failScore = eaLine._2._2 / totalNumberOfFailures
      val passScore = eaLine._2._1 / totalNumberOfPasses
      val score = failScore / (failScore + passScore)
      //LineRankings(lineNo) = score
      lineRankings.append((lineNo, score))
    }
    // Sort Array
    lineRankings.sortBy(_._2)(Ordering[Double].reverse)
    println(lineRankings.take(10))

    return lineRankings(0)._1
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