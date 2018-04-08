package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

object InputFacetsRepository {

  @BigQueryType.fromTable("adwords-dataflow:adwords_project_data_input.facets_input_federated")
  class InputFacetsRow

  def getInputFacets(sc: ScioContext, bq: BigQueryClient): SCollection[Seq[InputFacetsRow]] = {
    val facetsMap: Map[Any, List[InputFacetsRow]] =
      bq.getTypedRows[InputFacetsRow]().toList.groupBy(_.facet.get)

    sc.parallelize(
      facetsMap.keys.foldLeft(Seq[Seq[InputFacetsRow]]())((xs, x) => xs match {
        case Seq() => facetsMap(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsMap(x).map(_ +: comb))
      }).flatMap(_.permutations))
  }
}
