import collection.mutable.ArrayBuffer
import scala.tools.scalap.scalax.rules.scalasig.AnnotatedWithSelfType

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def updateTrace(x: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(1).getLineNumber
    (x+=newLine).distinct
  }

  def diverge(): CovFloat = {
    val copy = updateTrace(hist.clone())
    CovFloat(value, copy)
  }

  def mergeHistory(a: CovFloat): CovFloat = {
    CovFloat(value, (hist ++= a.hist).distinct)
  }

  def mergeHistory(a: CovString): CovFloat = {
    CovFloat(value, (hist ++= a.hist).distinct)
  }

  def *(x: Float): CovFloat = {
    CovFloat(value * x, updateTrace(hist))
  }

  def -(x: CovFloat): CovFloat = {
    CovFloat(value - x.value, (hist ++ x.hist).distinct)
  }

  def appendHistory(lineNum: Int): CovFloat = { //obsolete
    CovFloat(value, (hist+=lineNum).distinct)
  }
}

object CovFloat {
  implicit def ordering: Ordering[CovFloat] = Ordering.by(_.value)
}

