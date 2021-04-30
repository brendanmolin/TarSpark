import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by ali on 2/25/17.
 * Modified by BDE Team on Spring 2021
 */
object WeatherDataMmCount {
  def main(args: Array[String]) = {
    try {
      // Set up spark configuration
      val sparkConf = new SparkConf()
      sparkConf.setAppName("weatherData")
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")
      // Create Spark Context
      val ctx = new SparkContext(sparkConf)
      val lines = ctx.textFile("./data/moreFtData/allData", 1)
      val split = lines.flatMap{s =>
        var lineParts = s.split(",")
        val snowPart = lineParts(2)
        val snowUnit = snowPart.substring(snowPart.length - 2)
        List[(String, Int)](
          (snowUnit, 1)
        ).iterator
      }
      val output = split.reduceByKey((a,b)=>a+b).collect()
      println("Total number of output Records: " + output.length)
      for (o <- output){
        println(o)
      }

    }
  }

  def failure(record:Float): Boolean ={
    if (record > 6000f) {
      println("Failure record found! " + record)
    }
    record > 6000f
  }

  def zipToState(str : CovString):CovString = {
    // This is meant to change a zipcode to a state,
    // but currently just converts it to a number 0-49
    return (str.toInt % 50).toCovString
  }

}