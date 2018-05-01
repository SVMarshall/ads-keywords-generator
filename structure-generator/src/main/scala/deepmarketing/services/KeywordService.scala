package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Keyword, MatchType}
import deepmarketing.infrastructure.repositories.InputFacetsRepository
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet

object KeywordService {

  def generateKeywordsFromInputFacets(inputFacetsList: SCollection[Seq[InputFacetsRepository.InputFacet]])
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
        inputFacets.map(_.main_facet.getOrElse("")).mkString(" ").trim
      )
    })
  }

  private def generateKeywordCriteria(row: Seq[InputFacet]): String = {
    row.map(inputFacet => {
      if (inputFacet.field.get == "none") "" else inputFacet.field.get
    }).mkString(" ").trim
  }
}
