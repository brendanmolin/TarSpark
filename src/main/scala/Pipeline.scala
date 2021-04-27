trait Pipeline {
  def run(): Array[((String, String), CovFloat)] {}
  def failure(record: Float): Boolean {}
}
