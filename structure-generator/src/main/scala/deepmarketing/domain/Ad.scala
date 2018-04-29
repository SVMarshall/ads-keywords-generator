package deepmarketing.domain

case class Ad(h1: String, h2: String, description: String, url_display: String, adGroupName: String) {
  def csvEncode(): String = {
    Seq(adGroupName, h1, h2, description, url_display).map("\"" + _ + "\"").mkString(",")
  }
}
