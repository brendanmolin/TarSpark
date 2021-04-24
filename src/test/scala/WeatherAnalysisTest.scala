import org.scalatest.FunSuite

class WeatherAnalysisTest extends FunSuite {
  test("WeatherAnalysis.getFailedLine") {
    val totalNumberOfPasses = 8
    val totalNumberOfFails = 2
    val resultMap = collection.mutable.HashMap[Int, (Int, Int)](
      2 -> (2, 0),
      5 -> (8, 2),
      3 -> (6, 0)
    )
    assert(WeatherAnalysis.getFailedLine(resultMap, totalNumberOfPasses, totalNumberOfFails) == 5)
  }
}