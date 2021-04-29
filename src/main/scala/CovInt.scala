import collection.mutable.ArrayBuffer

case class CovInt(var value: Int, var hist: ArrayBuffer[Int]){

  def updateTrace(): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(3).getLineNumber
    val temp = hist.clone()
    (temp+=newLine).distinct
  }

  def %(x: Int): CovInt = {
    CovInt(value % x, updateTrace())
  }

  def toCovString: CovString = {
    CovString(value.toString, hist)
  }
}

