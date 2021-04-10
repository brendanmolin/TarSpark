import collection.mutable.ArrayBuffer

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def *(x: Float, lineNum: Int = -1): CovFloat = {
    if (lineNum != -1) {
      hist+=lineNum
    }
    CovFloat(value * x, hist)
  }

  def -(x: CovFloat, lineNum: Int = -1): CovFloat = {
    if (lineNum != -1) {
      hist+=lineNum
    }
    CovFloat(value - x.value, hist ++ x.hist)
  }

}

object CovFloat {
  implicit def ordering: Ordering[CovFloat] = Ordering.by(_.value)
}

