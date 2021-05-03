import collection.mutable.ArrayBuffer
import scala.tools.scalap.scalax.rules.scalasig.AnnotatedWithSelfType

case class CovFloat(value: Float, hist: ArrayBuffer[Int]) {

  def updateTrace(): ArrayBuffer[Int] = {
    val newLine = Thread.currentThread().getStackTrace()(3).getLineNumber
    val temp = hist.clone()
    (temp+=newLine).distinct
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
    CovFloat(value * x, updateTrace())
  }

  def -(x: CovFloat): CovFloat = {
    val temp2 = x.hist.clone()
    CovFloat(value - x.value, (temp2 ++= updateTrace()).distinct)
  }
}

object CovFloat {
  implicit def ordering: Ordering[CovFloat] = Ordering.by(_.value)
}

