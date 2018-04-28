package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

object InputFacetsRepository {

  def getInputFacets(sc: ScioContext, bq: BigQueryClient): SCollection[Seq[InputFacet]] = {
    val facetsGroupedByFacetName = bq.getTypedRows[InputFacet](
      "SELECT * FROM [adwords-dataflow.adwords_project_data_input.facets_input_federated] WHERE client = \"Trovimap\""
    ).toList.groupBy(_.facet.get)

    sc.parallelize(
      baseKeywordOrder.foldRight(Seq[Seq[InputFacet]]())((x, xs) => xs match {
        case Seq() => facetsGroupedByFacetName(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsGroupedByFacetName(x).map(_ +: comb))
      }))
  }

  // Ho hardcodegem. Hauriem de tenir un standard de fecets. En el cas del primer test la geo es deia zone.
  // El nom del facet que posem ara mateix depen de com es diu a la url.
  private def baseKeywordOrder: Set[String] = Set("action", "propertysubtypegroup", "geo", "minbeds")
  @BigQueryType.fromTable("adwords-dataflow:adwords_project_data_input.facets_input_federated")
  class InputFacet

}
