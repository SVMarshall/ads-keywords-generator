package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import deepmarketing.domain.{AdGroup, Keyword, MatchType}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacetsRow

object AdGroupRepository {

  def generateAdGroupsFromInputFacets(
                                       inputFacets: SCollection[Seq[InputFacetsRow]],
                                       matchType: MatchType): SCollection[AdGroup] = {
    inputFacets
      .map(row => {
        val text: String = generateKeywordText(row)
        val keyword: Keyword = Keyword(text, matchType)
        AdGroup(s"$text|$matchType",
          generateUrlLandingString(row),
          keyword,
          NegativeRepository.getBasicNegativesAdGroupLevel(keyword))
      })
  }

  private def generateKeywordText(row: Seq[InputFacetsRow]): String = {
    row.map(inputFacet => {
      if(inputFacet.field.get == "none") "" else inputFacet.field.get
    }).mkString(" ")
  }

  private def generateUrlLandingString(row: Seq[InputFacetsRow]): String = {
    "?" + row.map(inputFacet => {
      s"${inputFacet.facet.get}=${inputFacet.url_value.get}"
    }).mkString("&")
  }


  def generateTestAdGroups(sc: ScioContext): SCollection[AdGroup] = {
    sc.parallelize(
      IndexedSeq(
        AdGroup("piso barcelona | BRD", "www.pepe.com", Keyword("piso barcelona", new MatchType("BRD")), Seq()),
        AdGroup("piso barcelona | PHR", "www.pepe.com", Keyword("piso barcelona", new MatchType("PHR")), Seq()),
        AdGroup("piso barcelona | EXT", "www.pepe.com", Keyword("piso barcelona", new MatchType("EXT")), Seq()),
        AdGroup(
          "piso barcelona 3 habs | BRD", "www.pepe.com", Keyword("piso barcelona 3 habs", new MatchType("BRD")), Seq()),
        AdGroup(
          "piso barcelona 3 habs | PHR", "www.pepe.com", Keyword("piso barcelona 3 habs", new MatchType("PHR")), Seq()),
        AdGroup(
          "piso barcelona 3 habs | EXT", "www.pepe.com", Keyword("piso barcelona 3 habs", new MatchType("EXT")), Seq())
      )
    )
  }
}
