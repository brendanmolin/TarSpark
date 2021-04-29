import collection.mutable.ArrayBuffer

case class CovString(var value: String, var hist: ArrayBuffer[Int]){

  def updateTrace(): CovString = {
    val newLine = Thread.currentThread().getStackTrace()(2).getLineNumber
    CovString(value, (hist+=newLine).distinct)
  }

  /*def appendHistory(lineNum: Int): CovString = {
    CovString(value, (hist+=lineNum).distinct)
  }*/

  def split(separator: String): Array[CovString] = {
    value
      .split(separator)
      .map(s =>
        CovString(
          s, hist))
  }

  def substring(beginIndex: Int): CovString = {
    val temp = hist.clone()
    val valueStr = value.substring(beginIndex)
    CovString(valueStr, temp)
  }

  def substring(beginIndex: Int, endIndex: Int): CovString = {
    val temp = hist.clone()
    var valueStr = value.substring(beginIndex, endIndex)
    CovString(valueStr, temp)
  }

  def mergeHistory(a: CovFloat): CovString = {
    val temp = hist.clone()
    CovString(value, (temp ++= a.hist).distinct)
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

  /*def diverge(): CovString = {
  val copy = updateTrace(hist.clone())
  CovString(value, copy)
}*/

}

