package deepmarketing

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import common.implicits.DateTimeFormatters._
import deepmarketing.domain.{Ad, InputFacet, Keyword, Negative}
import deepmarketing.infrastructure.repositories._
import deepmarketing.services.{AdService, KeywordService, NegativeService}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object KeywordsPipeline {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    val clientConfigSheet: String = args.optional("config").getOrElse("1bNG0OfnuCMSiJVnMfZssCUEJlQXWPphUKCovedFcTpE")

    val date: DateTime = DateTime.now()
    val execPath: String = s"gs://adwords-dataflow/structure-generator/exec/${date.toTimestamp}"

    val inputFacets: SCollection[Seq[InputFacet]] =
      new InputFacetsRepository(clientConfigSheet).getInputFacets(sc, clientConfigSheet)
    val keywords: SCollection[Keyword] = KeywordService.generateKeywordsFromInputFacets(inputFacets)
    val ads: SCollection[Ad] = AdService.generateAds(keywords, clientConfigSheet)
    val negatives: SCollection[Negative] = NegativeService.generateNegatives(keywords)


    keywords.saveAsObjectFile(s"$execPath/keywords")
    negatives.saveAsObjectFile(s"$execPath/negatives")
    ads.saveAsObjectFile(s"$execPath/ads")

    val result: ScioResult = sc.close().waitUntilFinish(60, MINUTES)
  }
}
