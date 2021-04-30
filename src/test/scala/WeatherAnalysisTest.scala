import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class WeatherAnalysisTest extends FunSuite {
  test("TarSpark.getFailedLine") {
    val totalNumberOfPasses = 8
    val totalNumberOfFails = 2
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)](
      2 -> (2, 0),
      5 -> (8, 2),
      3 -> (6, 0)
    )
    val tarspark = TarSpark(new WeatherAnalysis)
    assert(tarspark.getFailedLine(resultMap, totalNumberOfPasses, totalNumberOfFails) == 5)
    val resultMap2 = collection.mutable.HashMap[Int, (Int, Int)](
      5 -> (6, 2),
      3 -> (6, 2),
      2 -> (2, 0)
    )
    // In the case of a score tie, choose the earliest lineNo
    assert(tarspark.getFailedLine(resultMap2, totalNumberOfPasses, totalNumberOfFails) == 3)

    val resultMap3 = collection.mutable.HashMap[Int, (Int, Int)](
      0 -> (8, 2),
      2 -> (8, 0),
      3 -> (0, 2),
      5 -> (8, 2)
    )
    assert(tarspark.getFailedLine(resultMap3, totalNumberOfPasses, totalNumberOfFails) == 3)
  }

  test("WeatherAnalysis.summarizeFailLines") {
    /*
       0 10 records total
       /        \
    line 2: 8   line 3: 2 Failure @ line 3
       \          /
        line 5: 10

     Final:
      (key, goodValue), (0, 2, 5) x 8
      (key, badValue), (0, 3, 5) x 2
     */
    val tarspark = TarSpark(new WeatherAnalysis)
    val resultList = Array[((String, String), CovFloat)](
      (("keyA1", "keyB1"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA2", "keyB2"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA3", "keyB3"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA4", "keyB4"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA5", "keyB5"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA6", "keyB6"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA7", "keyB7"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA8", "keyB8"), CovFloat(100, ArrayBuffer[Int](0, 2, 5))),
      (("keyA9", "keyB9"), CovFloat(7000, ArrayBuffer[Int](0, 3, 5))),
      (("keyA10", "keyB10"), CovFloat(7000, ArrayBuffer[Int](0, 3, 5)))
    )
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)](
      0 -> (8, 2),
      2 -> (8, 0),
      3 -> (0, 2),
      5 -> (8, 2)
    )
    val totalNumberOfPasses = 8
    val totalNumberOfFailures = 2
    assert(tarspark.summarizeFailLines(resultList) == (resultMap, totalNumberOfPasses, totalNumberOfFailures))
  }
}