import collection.mutable.ArrayBuffer

case class CovString(var value: String, var hist: ArrayBuffer[Int]){

  def updateTrace(): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(3).getLineNumber
    val temp = hist.clone()
    (temp+=newLine).distinct
  }

  def mergeHistory(a: CovFloat): CovString = {
    val temp = hist.clone()
    CovString(value, (temp ++= a.hist).distinct)
  }

  def split(separator: String): Array[CovString] = {
    val newLine = updateTrace()
    value.split(separator).map(s => CovString(s, newLine.clone()))
  }

  def substring(beginIndex: Int): CovString = {
    val valueStr = value.substring(beginIndex)
    CovString(valueStr, updateTrace())
  }

  def substring(beginIndex: Int, endIndex: Int): CovString = {
    var valueStr = value.substring(beginIndex, endIndex)
    CovString(valueStr, updateTrace())
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

