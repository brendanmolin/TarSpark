import collection.mutable.ArrayBuffer

case class CovString(var value: String, var hist: ArrayBuffer[Int]){

  def updateTrace(x: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(3).getLineNumber
    (x+=newLine).distinct
  }

  def appendHistory(lineNum: Int): CovString = {
    CovString(value, (hist+=lineNum).distinct)
  }

  def split(separator: String): Array[CovString] = {
    value
      .split(separator)
      .map(s =>
        CovString(
          s, hist))
  }

  def substring(beginIndex: Int): CovString = {
    val valueStr = value.substring(beginIndex)
    CovString(valueStr, hist)
  }

  def substring(beginIndex: Int, endIndex: Int): CovString = {
    var valueStr = value.substring(beginIndex, endIndex)
    CovString(valueStr, hist)
  }

  def diverge(): CovString = {
    val copy = updateTrace(hist.clone())
    CovString(value, copy)
  }

  def mergeHistory(a: CovFloat): CovString = {
    CovString(value, (hist ++= a.hist).distinct)
  }

  def toInt(): CovInt = {
    CovInt(value.toInt, hist)
  }

  def toFloat(): CovFloat = {
    CovFloat(value.toFloat, hist)
  }

  def length(): Int = {
    value.length()
  }
}

