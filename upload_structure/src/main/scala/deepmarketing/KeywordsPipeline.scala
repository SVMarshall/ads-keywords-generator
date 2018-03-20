package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.BigQueryClient

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

case class Keyword(name: String, matchType: String, urlLanding: String)

object KeywordsPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputFacets = getInputKeywords()

    // example of use:
    val keywordsAndMatchTypes = inputFacets
      //.map(_.map(_.field.get).mkString(" "))
      .flatMap(row => {
        val text = row.map(_.field.get).mkString(" ")
        val url = s"http://blablalbal${row.map(_.url_value.get).mkString("&")}"
      Seq(Keyword(text, "BRD", url),
          Keyword(text, "EXT", url),
          Keyword(text, "PHR", url))
    })

    //val negatives = Negatives.transform(keywords)
    //negatives.saveAsAvroFile("negativesPath")

  }

  @BigQueryType.fromTable("adwords-dataflow:adwords_project_data_input.facets_input_federated")
  class InputFacetsRow

  def getInputKeywords()(implicit sc: ScioContext): SCollection[Seq[InputFacetsRow]] = {
    val bq = BigQueryClient.defaultInstance()
    val facetsMap = bq.getTypedRows[InputFacetsRow]().toList.groupBy(_.facet.get)

    sc.parallelize(
      facetsMap.keys.foldLeft(Seq[Seq[InputFacetsRow]]())((xs, x) => xs match {
        case Seq() => facetsMap(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsMap(x).map(_ +: comb))
      }).flatMap(_.permutations))
  }

  // Word Count:
  /*

  List(

     "badalona venta vivienda",

     )


  val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
  val input = args.getOrElse("input", exampleData)
  val output = args("output")

  sc.textFile(input)
    .map(_.trim)
    .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    .countByValue
    .map(t => t._1 + ": " + t._2)
    .saveAsTextFile(output)

  val result = sc.close().waitUntilFinish()
  */
}
