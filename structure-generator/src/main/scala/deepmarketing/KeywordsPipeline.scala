package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Ad, Keyword, Negative}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet
import deepmarketing.infrastructure.repositories._
import deepmarketing.services.{AdService, KeywordService, NegativeService}
import org.joda.time.DateTime
import common.implicits.DateTimeFormatters._

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
    val date = DateTime.now()
    val execPath = s"gs://adwords-dataflow/keywords/structure-generator/exec/${date.toTimestamp}"

    //Get Input Facets
    //Create Keywords
    //Generate Adgroup Name
    //Generate Campaign Name
    //Generate Ads

    //val baseAdGroups = AdGroupRepository.generateTestAdGroups(sc)
    //val negatives: SCollection[(String, Seq[Negative])] = NegativeRepository.generateNegatives(baseAdGroups)
    //Get Negatives
    //    keywordsWithAds.map(_.ads.head).saveAsTextFile("gs://adwords-dataflow/ads")
    //    val io: IoCommands = new IoCommands(sc.options)
    //implicit val personEncoder: RowEncoder[Person] = RowEncoder.caseEncoder(0, 2, 1)(Person.unapply)
    //    implicit val adEncoder: RowEncoder[Ad] = RowEncoder.caseEncoder(0,1,2,3)(Ad.unapply)
    //    io.writeCsv[Ad]("gs://adwords-dataflow/keywords", keywordsWithAds.flatMap(_.ads));

    val inputFacets: SCollection[Seq[InputFacet]] = InputFacetsRepository.getInputFacets(sc, bq)
    val keywords: SCollection[Keyword] = KeywordService.generateKeywordsFromInputFacets(inputFacets)
    val ads: SCollection[Ad] = AdService.generateAds(keywords)
    val negatives: SCollection[Negative] = NegativeService.generateNegatives(keywords)
    //keywords.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/keywords")
    //negatives.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/negatives")
    //negatives.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/negatives")

    keywords.saveAsObjectFile(s"${execPath}/keywords")
    negatives.saveAsObjectFile(s"${execPath}/negatives")
    ads.saveAsObjectFile(s"${execPath}/ads")


    sc.close()
  }
}
