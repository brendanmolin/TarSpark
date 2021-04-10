import collection.mutable.ArrayBuffer

case class CovInt(var value: Int, var hist: ArrayBuffer[Int]){

  def %(x: Int, lineNum: Int = -1): CovInt = {
    if (lineNum != -1) {
      hist+=lineNum
    }
    CovInt(value % x, hist)
  }

  def toCovString: CovString = {
    CovString(value.toString, hist)
  }
}

