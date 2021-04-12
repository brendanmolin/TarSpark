import collection.mutable.ArrayBuffer

case class CovString(var value: String, var histString: ArrayBuffer[Int]){

  def split(separator: String, lineNum: Int = -1): Array[CovString] = {
    if (lineNum != -1) {
      histString +=lineNum
    }

    value
      .split(separator)
      .map(s =>
        CovString(
          s, histString))
  }

  def substring(beginIndex: Int): CovString = {
    val valueStr = value.substring(beginIndex)
    CovString(valueStr, histString)
  }
  def dateSubstring(beginIndex: Int, lineNum: Int): CovString = {
    val valueStr = value.substring(beginIndex)
    CovString(valueStr, histString += lineNum)
  }
  def substring(beginIndex: Int, endIndex: Int): CovString = {
    var valueStr = value.substring(beginIndex, endIndex)
    CovString(valueStr, histString)
  }
  def dateSubstring(beginIndex: Int, endIndex: Int, lineNum: Int): CovString = {
    var valueStr = value.substring(beginIndex, endIndex)
    CovString(valueStr, histString+= lineNum)
  }

  def divergeStrCopy(x: CovString): CovString = {
    //val xNew = x.copy(hist += lineNum)
    CovString(x.value, x.histString.clone())
  }

  def mergeHistory(a: CovFloat, b: CovString): CovFloat = {
    CovFloat(a.value, a.hist ++= b.histString)
  }

  def toInt(): CovInt = {
    CovInt(value.toInt, histString)
  }

  def toFloat(): CovFloat = {
    CovFloat(value.toFloat, histString)
  }

  def length(): Int = {
    value.length()
  }
}

