package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Keyword, MatchType, AdGroup}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacetsRow

object AdGroupRepository {

  def generateAdGroupsFromInputFacets(inputFacets: SCollection[Seq[InputFacetsRow]]): SCollection[AdGroup] = {
    inputFacets
      .flatMap(row => {
        createAdgroupsForAllMatchTypes(generateKeywordText(row), generateUrlLandingString(row))
      })
  }

  private def generateKeywordText(row: Seq[InputFacetsRow]): String = {
    row.map(_.field.get).mkString(" ")
  }

  private def generateUrlLandingString(row: Seq[InputFacetsRow]): String = {
    "?" + row.map(inputFacet => {
      s"${inputFacet.facet.get}=${inputFacet.url_value.get}"
    }).mkString("&")
  }

  private def createAdgroupsForAllMatchTypes(text: String, url: String) = {
    Seq("BRD", "EXT", "PHR").map(matchType => {
      AdGroup(s"$text|$matchType", url, Keyword(text, new MatchType(matchType)), Seq())
    })
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
