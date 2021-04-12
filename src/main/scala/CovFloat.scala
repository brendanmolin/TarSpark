import collection.mutable.ArrayBuffer

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def appendHistory(lineNum: Int): CovFloat = {
    CovFloat(value, (hist+=lineNum).distinct)
  }

  def diverge(): CovFloat = {
    CovFloat(value, hist.clone())
  }

  def mergeHistory(a: CovFloat): CovFloat = {
    CovFloat(value, (hist ++= a.hist).distinct)
  }

  def mergeHistory(a: CovString): CovFloat = {
    CovFloat(value, (hist ++= a.hist).distinct)
  }

  def *(x: Float): CovFloat = {
    CovFloat(value * x, hist)
  }

  def -(x: CovFloat): CovFloat = {
    CovFloat(value - x.value, (hist ++ x.hist).distinct)
  }
}

object CovFloat {
  implicit def ordering: Ordering[CovFloat] = Ordering.by(_.value)
}

