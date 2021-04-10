import collection.mutable.ArrayBuffer

case class CovString(var value: String, var hist: ArrayBuffer[Int]){

  def split(separator: String, lineNum: Int = -1): Array[CovString] = {
    if (lineNum != -1) {
      hist.append(lineNum)
    }

    value
      .split(separator)
      .map(s =>
        CovString(
          s, hist))
  }
}

