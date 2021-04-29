import collection.mutable.ArrayBuffer
import scala.tools.scalap.scalax.rules.scalasig.AnnotatedWithSelfType

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def updateTrace(x: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(3).getLineNumber
    (x+=newLine).distinct
  }

  def mergeHistory(a: CovFloat): CovFloat = {
    val temp = hist.clone()
    CovFloat(value, (temp ++= a.hist).distinct)
  }

  def mergeHistory(a: CovString): CovFloat = {
    val temp = hist.clone()
    CovFloat(value, (temp ++= a.hist).distinct)
  }

  def *(x: Float): CovFloat = {
    CovFloat(value * x, updateTrace(hist))
  }

  def -(x: CovFloat): CovFloat = {
    val temp = hist.clone()
    CovFloat(value - x.value, (temp ++ x.hist).distinct)
  }

  /*def diverge(): CovFloat = {
  val x = hist.clone()
  val copy = updateTrace(hist.clone())
  CovFloat(value, copy)
}*/

  /*def appendHistory(lineNum: Int): CovFloat = { //obsolete
    CovFloat(value, (hist+=lineNum).distinct)
  }*/
}

object CovFloat {
  implicit def ordering: Ordering[CovFloat] = Ordering.by(_.value)
}

