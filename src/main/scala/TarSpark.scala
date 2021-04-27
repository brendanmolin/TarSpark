import scala.collection.mutable.ArrayBuffer

case class TarSpark(pipeline: Pipeline) {

  def main() = {
    val output = pipeline.run()
    val failLinesOutput = summarizeFailLines(output)
    val failLineOutputMap = failLinesOutput._1
    val totalNumberOfPasses = failLinesOutput._2
    val totalNumberOfFailures = failLinesOutput._3
    println("FailureLines output map: " + failLineOutputMap)
    println("totalNumberOfPasses: " + totalNumberOfPasses)
    println("totalNumberOfFailures: " + totalNumberOfFailures)
    println("Suggested line with bug: " + getFailedLine(failLineOutputMap, totalNumberOfPasses, totalNumberOfFailures))
  }

  def summarizeFailLines(resultList:Array[((String, String), CovFloat)]): (collection.mutable.HashMap[Int, (Int, Int)], Int, Int) = {
    // (lineNo, (No of passing records, No of failing records)
    var totalNumberOfPasses = resultList.length
    var totalNumberOfFailures = 0
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)]() // Create new empty Map
    for (o <- resultList){
      val isFailure = pipeline.failure(o._2.value)
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
    var lineRankings = ArrayBuffer[(Int, Double)]() //Create new list of Tuples
    for (eaLine <- resultMap){
      val lineNo = eaLine._1
      if (totalNumberOfPasses == 0){
        lineRankings.append((lineNo, 1))
      }
      else {
        val failScore = eaLine._2._2.toDouble / totalNumberOfFailures
        val passScore = eaLine._2._1.toDouble / totalNumberOfPasses
        val score = failScore / (failScore + passScore)
        lineRankings.append((lineNo, score))
      }
    }
    // Sort Array
    val sortedLineRankings1 = lineRankings.sortBy(_._1)(Ordering[Int]) // First sort by LineNo
    val sortedLineRankings2 = sortedLineRankings1.sortBy(_._2)(Ordering[Double].reverse) // Then sort by Score
    println("Line Rankings: (LineNo, Suspicious Score)")
    println(sortedLineRankings2)
    return sortedLineRankings2(0)._1
  }


}
