package deepmarketing.domain

case class MatchType(text: String) {

  private val matchTypesHierarchy: Map[String, Int] = Map("BRD" -> 1, "PHR" -> 2, "EXT" -> 3)

  def isGreaterOrEqual(matchTypeCompare: MatchType): Boolean = {
    matchTypesHierarchy(this.text) >= matchTypesHierarchy(matchTypeCompare.text)
  }

  def >(matchTypeCompare: MatchType): Boolean = {
    matchTypesHierarchy(this.text) > matchTypesHierarchy(matchTypeCompare.text)
  }
}
