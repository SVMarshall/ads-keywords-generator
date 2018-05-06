package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import common.implicits.DateTimeFormatters._
import deepmarketing.domain.{Ad, Keyword, Negative}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet
import deepmarketing.infrastructure.repositories._
import deepmarketing.services.{AdService, KeywordService, NegativeService}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object KeywordsPipeline {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val bq: BigQueryClient = BigQueryClient.defaultInstance()
    val date = DateTime.now()
    val execPath = s"gs://adwords-dataflow/structure-generator/exec/${date.toTimestamp}"

    val inputFacets: SCollection[Seq[InputFacet]] = InputFacetsRepository.getInputFacets(sc, bq)
    val keywords: SCollection[Keyword] = getKeywords(inputFacets)
    val ads: SCollection[Ad] = getAds(keywords)
    val negatives: SCollection[Negative] = getNegatives(keywords)


    keywords.saveAsObjectFile(s"$execPath/keywords")
    negatives.saveAsObjectFile(s"$execPath/negatives")
    ads.saveAsObjectFile(s"$execPath/ads")

    sc.close().waitUntilDone(60, MINUTES)
  }

  def getInputFacets(sc: ScioContext, bq: BigQueryClient): SCollection[Seq[InputFacet]] = {
    InputFacetsRepository.getInputFacets(sc, bq)
  }

  def getKeywords(inputFacets: SCollection[Seq[InputFacet]]): SCollection[Keyword] = {
    KeywordService.generateKeywordsFromInputFacets(inputFacets)
  }

  def getAds(keywords: SCollection[Keyword]): SCollection[Ad] = {
    AdService.generateAds(keywords)
  }

  def getNegatives(keywords: SCollection[Keyword]): SCollection[Negative] = {
    NegativeService.generateNegatives(keywords)
  }
}
