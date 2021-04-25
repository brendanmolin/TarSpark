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
        // finds the state for a zipcode
        var state = zipToState(covtokens(0))
        var date = covtokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(covtokens(2)) // CAPTURE HERE)
        //gets year
        val year = date.diverge().substring(date.value.lastIndexOf("/") + 1).appendHistory(Thread.currentThread().getStackTrace()(1).getLineNumber)
        // gets month / date
        val monthdate= date.diverge().substring(0, date.value.lastIndexOf("/")).appendHistory(Thread.currentThread().getStackTrace()(1).getLineNumber)
        List[((String , String) , CovFloat)](
          ((state.value , monthdate.value) , snow.diverge().mergeHistory(monthdate)) ,
          ((state.value , year.value)  , snow.diverge().mergeHistory(year))
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val s1 = s._2
        val s2 = s._2
        val delta =  s1.max - s2.min
        (s._1 , delta)
      }

      val output = deltaSnow.collect()
      val failLinesOutput = summarizeFailLines(output)
      val failLineOutputMap = failLinesOutput._1
      val totalNumberOfPasses = failLinesOutput._2
      val totalNumberOfFailures = failLinesOutput._3
      println("FailureLines output map: " + failLineOutputMap)
      println("totalNumberOfPasses: " + totalNumberOfPasses)
      println("totalNumberOfFailures: " + totalNumberOfFailures)
      println("Suggested line with bug:" + getFailedLine(failLineOutputMap, totalNumberOfPasses, totalNumberOfFailures))
    }
  }

  def convert_to_mm(s: CovString): CovFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit.value match {
      case "mm" => return v.appendHistory(Thread.currentThread().getStackTrace()(1).getLineNumber)
      //case "inch" => return ...
      case _ => return (v * 304.8f).appendHistory(Thread.currentThread().getStackTrace()(1).getLineNumber)
    }
  }

  def summarizeFailLines(resultList:Array[((String, String), CovFloat)]): (collection.mutable.HashMap[Int, (Int, Int)], Int, Int) = {
    // (lineNo, (No of passing records, No of failing records)
    var totalNumberOfPasses = resultList.length
    println("Total number of results in output: "+ totalNumberOfPasses)
    var totalNumberOfFailures = 0
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)]() // Create new empty Map
    for (o <- resultList){
      val isFailure = failure(o._2.value)
      if (isFailure){
        println("Failure found: " + o)
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
      println("eaLine: " + eaLine)
      val lineNo = eaLine._1
      println("\tlineNo: " + lineNo)
      if (totalNumberOfPasses == 0){
        lineRankings.append((lineNo, 1))
      }
      else {
        val failScore = eaLine._2._2.toDouble / totalNumberOfFailures
        println("\t\teaLine._2._2: "+ eaLine._2._2)
        println("\t\ttotalNuberofFailures: " + totalNumberOfFailures)
        println("\t\tfailScore: " + failScore)
        val passScore = eaLine._2._1.toDouble / totalNumberOfPasses
        println("\t\teaLine._2._1: "+ eaLine._2._1)
        println("\t\ttotalNumberOfPasses: " + totalNumberOfPasses)
        println("\t\tpassScore: " + passScore)
        val score = failScore / (failScore + passScore)
        println("\t\tscore: " + score)
        lineRankings.append((lineNo, score))
      }
    }
    // Sort Array
    println("lineRankings: " + lineRankings)
    val sortedLineRankings1 = lineRankings.sortBy(_._1)(Ordering[Int]) //First sort by LineNo
    val sortedLineRankings2 = sortedLineRankings1.sortBy(_._2)(Ordering[Double].reverse) //Then sort by Score
    println("sortedLineRankings: " + sortedLineRankings2)
    return sortedLineRankings2(0)._1
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