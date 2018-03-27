package deepmarketing.structure

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

case class Keyword(text: String, matchType: String)
case class Negative(text: String, matchType: String)
case class Adgroup(name: String, urlLanding: String, keyword: Keyword, negatives: Seq[Negative])

@BigQueryType.fromTable("adwords-dataflow:adwords_project_data_input.facets_input_federated")
class InputFacetsRow

object BaseAdgroups {
  def get()(implicit sc: ScioContext): SCollection[Adgroup] = {
    val bq = BigQueryClient.defaultInstance()
    val facetsMap = bq.getTypedRows[InputFacetsRow]().toList.groupBy(_.facet.get)

    val inputFacets = sc.parallelize(
      facetsMap.keys.foldLeft(Seq[Seq[InputFacetsRow]]())((xs, x) => xs match {
        case Seq() => facetsMap(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsMap(x).map(_ +: comb))
      }).flatMap(_.permutations))

    val adgroupsWithoutNegatives = inputFacets
      //.map(_.map(_.field.get).mkString(" "))
      .flatMap(row => {
      val text = row.map(_.field.get).mkString(" ")
      val url = s"http://blablalbal${row.map(_.url_value.get).mkString("&")}"
      Seq("BRD", "EXT", "PHR").map(matchType => {
        Adgroup(s"${text}|${matchType}", url, Keyword(text, matchType), Seq())
      })
    })

    // negatives
    val adgroups = Negatives.generateNegatives(adgroupsWithoutNegatives)


  }

}