package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import deepmarketing.common.spreadsheet.SheetsBuilder
import deepmarketing.domain.InputFacet

class InputFacetsRepository(clientConfigSheet: String) {

   val inputFacetsValues: List[List[String]] =
    SheetsBuilder.build(clientConfigSheet).getValues("input_facet", "A2:E1000000")

   val header: List[String] =
    SheetsBuilder.build(clientConfigSheet).getValues("input_facet", "A1:E1").head

  def getInputFacets(sc: ScioContext): SCollection[Seq[InputFacet]] = {

    val facetsGroupedByFacetName: Map[String, List[InputFacet]] = composeInputFacets.toList.groupBy(_.facet)

    sc.parallelize(
      facetsConfig.foldRight(Seq[Seq[InputFacet]]())((x, xs) => xs match {
        case Seq() => facetsGroupedByFacetName(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsGroupedByFacetName(x).map(_ +: comb))
      }))
  }

  private def composeInputFacets: Seq[InputFacet] = {
    inputFacetsValues
      .map(row => {
        InputFacet(
          if (row.length > header.indexOf("facet")) row(header.indexOf("facet")) else "",
          if (row.length > header.indexOf("field")) row(header.indexOf("field")) else "",
          if (row.length > header.indexOf("url_value")) row(header.indexOf("url_value")) else "",
          if (row.length > header.indexOf("main_facet")) row(header.indexOf("main_facet")) else "",
          if (row.length > header.indexOf("url_name")) row(header.indexOf("url_name")) else "")
      })
  }

  def facetsConfig: Set[String] = Set("action", "type", "geo", "rooms")
}
