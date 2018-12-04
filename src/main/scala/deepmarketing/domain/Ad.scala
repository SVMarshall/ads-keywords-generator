package deepmarketing.domain

case class Ad(h1: String,
              h2: String,
              description: String,
              url_display_1: String,
              url_display_2: String,
              adGroupName: String,
              finalUrl: String
             ) {
  def csvEncode: String = Seq(
    adGroupName,
    h1,
    h2,
    description,
    url_display_1,
    url_display_2,
    finalUrl).map("\"" + _ + "\"").mkString(",")
}