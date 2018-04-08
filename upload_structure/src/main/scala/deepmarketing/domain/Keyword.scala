package deepmarketing.domain

case class Keyword(text: String, matchType: MatchType) extends KeywordGeneric {
  def toNegative: Negative = Negative(text, matchType)
}
