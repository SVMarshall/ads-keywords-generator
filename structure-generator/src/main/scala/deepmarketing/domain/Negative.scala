package deepmarketing.domain

case class Negative(criteria: String, matchType: MatchType, adGroupName: String) {

  def csvEncode(): String = {
    Seq(criteria, matchType.text, adGroupName).map("\"" + _ + "\"").mkString(",")
  }
}
