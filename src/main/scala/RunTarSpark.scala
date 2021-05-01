
object RunTarSpark {
  def main(args: Array[String]): Unit = {
    val myPipe: Pipeline = new WeatherAnalysis
    TarSpark(myPipe).main()
  }
}
