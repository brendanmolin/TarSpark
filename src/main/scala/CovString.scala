import collection.mutable.ArrayBuffer

case class CovString(var value: String, var hist: ArrayBuffer[Int]){

  def split(separator: String, lineNum: Int = -1): Array[CovString] = {
    if (lineNum != -1) {
      hist+=lineNum
    }

    value
      .split(separator)
      .map(s =>
        CovString(
          s, hist))
  }

  def substring(beginIndex: Int, lineNum: Int = -1): CovString = {
    if (lineNum != -1) {
      hist+=lineNum
    }
    var valueStr = value
    valueStr = valueStr.substring(beginIndex)
    CovString(valueStr, hist)
  }

  def substring(beginIndex: Int, endIndex: Int, lineNum: Int = -1): CovString = {
    if (lineNum != -1) {
      hist+=lineNum
    }
    var valueStr = value
    valueStr = valueStr.substring(beginIndex, endIndex)
    CovString(valueStr, hist)
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

