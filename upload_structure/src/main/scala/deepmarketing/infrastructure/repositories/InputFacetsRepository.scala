package deepmarketing.infrastructure.repositories

import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.types.BigQueryType

object InputFacetsRepository {

  def getInputFacets(bq: BigQueryClient): Map[Any, List[InputFacetsRow]] = {
    bq.getTypedRows[InputFacetsRow](
      "SELECT * FROM [adwords-dataflow.adwords_project_data_input.facets_input_federated] WHERE client = \"Trovimap\""
    ).toList.groupBy(_.facet.get)
  }

  @BigQueryType.fromTable("adwords-dataflow:adwords_project_data_input.facets_input_federated")
  class InputFacetsRow

}
