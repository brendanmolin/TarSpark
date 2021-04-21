import collection.mutable.ArrayBuffer

case class CovInt(var value: Int, var hist: ArrayBuffer[Int]){

  def %(x: Int): CovInt = {
    CovInt(value % x, hist)
  }
//add comment
  def toCovString: CovString = {
    CovString(value.toString, hist)
  }
}

