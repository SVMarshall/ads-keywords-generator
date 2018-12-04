package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{InputFacet, Keyword, MatchType}

object KeywordService {

  def generateKeywordsFromInputFacets(inputFacetsList: SCollection[Seq[InputFacet]])
  : SCollection[Keyword] = {
    inputFacetsList
      .flatMap(inputFacets => {
        createKeywordsForAllMatchTypes(inputFacets)
      })
  }

  private def createKeywordsForAllMatchTypes(inputFacets: Seq[InputFacet]): Seq[Keyword] = {
    Seq("BROAD", /*"PHRASE", */"EXACT").map(matchType => {
      val criteria: String = generateKeywordCriteria(inputFacets)
      Keyword(
        inputFacets,
        criteria,
        MatchType(matchType),
        s"$criteria|$matchType",
        inputFacets.map(_.main_facet).mkString(" ").trim
      )
    })
  }

  private def generateKeywordCriteria(row: Seq[InputFacet]): String = {
    row.map(inputFacet => {
      if (inputFacet.field == "none") "" else inputFacet.field
    }).mkString(" ").trim
  }
}
