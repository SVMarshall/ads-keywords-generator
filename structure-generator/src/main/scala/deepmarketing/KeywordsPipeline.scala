package deepmarketing

import com.spotify.scio._
import com.spotify.scio.repl.IoCommands
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Ad, Keyword}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet
import deepmarketing.infrastructure.repositories._
import deepmarketing.services.{AdService, KeywordService}
import kantan.csv.RowEncoder

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object KeywordsPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val bq: BigQueryClient = BigQueryClient.defaultInstance()

    //Get Input Facets
    //Create Keywords
    //Generate Adgroup Name
    //Generate Campaign Name
    //Generate Ads

    //val baseAdGroups = AdGroupRepository.generateTestAdGroups(sc)
    //val negatives: SCollection[(String, Seq[Negative])] = NegativeRepository.generateNegatives(baseAdGroups)
    //Get Negatives

    val inputFacets: SCollection[Seq[InputFacet]] = InputFacetsRepository.getInputFacets(sc, bq)
    val keywords: SCollection[Keyword] = KeywordService.generateKeywordsFromInputFacets(inputFacets)
    val keywordsWithAds: SCollection[Keyword] = AdService.addAds(keywords)
//    keywordsWithAds.map(_.ads.head).saveAsTextFile("gs://adwords-dataflow/ads")

    val io: IoCommands = new IoCommands(sc.options)

    //implicit val personEncoder: RowEncoder[Person] = RowEncoder.caseEncoder(0, 2, 1)(Person.unapply)

    implicit val adEncoder: RowEncoder[Ad] = RowEncoder.caseEncoder(0,1,2,3)(Ad.unapply)

    keywordsWithAds.map(keyword => io.writeCsv[Ad]("gs://adwords-dataflow/keywords", keyword.ads))
    sc.close()
  }
}
