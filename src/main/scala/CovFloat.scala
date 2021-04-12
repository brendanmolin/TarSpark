import collection.mutable.ArrayBuffer

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def appendHistory(lineNum: Int): CovFloat = {
    CovFloat(value, hist+=lineNum)
  }
  def divergeFloatCopy(x: CovFloat): CovFloat = {
    //val xNew = x.copy(hist += lineNum)
    CovFloat(x.value, x.hist.clone())
  }

  def mergeHistory(a: CovFloat, b: CovString): CovFloat = {
    CovFloat(a.value, a.hist ++= b.histString)
  }

  def distinctHist(a: CovFloat): CovFloat = {
    CovFloat(a.value, hist.distinct)
  }
  def *(x: Float, lineNum: Int = -1): CovFloat = { //TODO remove lineNum parameter and check, use append history or reWrap as new CovFloat
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

