package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Keyword, MatchType, Negative}

object NegativeService {

  def generateNegatives(keywords: SCollection[Keyword]): SCollection[Negative] = {
    keywords.flatMap(keyword => getBasicNegativesAdGroupLevel(keyword))
  }

  def getBasicNegativesAdGroupLevel(keyword: Keyword): Seq[Negative] = {
    if (keyword.matchType.text == "BROAD") {
      Seq(
        Negative(keyword.criteria, new MatchType("PHRASE"), keyword.adGroupName),
        Negative(keyword.criteria, new MatchType("EXACT"), keyword.adGroupName)
      )
    } else if (keyword.matchType.text == "PHRASE") {
      Seq(Negative(keyword.criteria, new MatchType("EXACT"), keyword.adGroupName))
    } else {
      Seq()
    }
  }
}
