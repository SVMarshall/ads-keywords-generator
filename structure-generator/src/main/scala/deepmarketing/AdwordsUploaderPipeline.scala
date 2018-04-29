package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Ad, Keyword, Negative}
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet
import deepmarketing.infrastructure.repositories._
import deepmarketing.services.{AdService, KeywordService, NegativeService}


object AdwordsUploaderPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val bq: BigQueryClient = BigQueryClient.defaultInstance()

    //val inputFacets: SCollection[Seq[InputFacet]] = InputFacetsRepository.getInputFacets(sc, bq)
    //val keywords: SCollection[Keyword] = KeywordService.generateKeywordsFromInputFacets(inputFacets)
    //val ads: SCollection[Ad] = AdService.generateAds(keywords)
    //val negatives: SCollection[Negative] = NegativeService.generateNegatives(keywords)

    //keywords.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/keywords")
    //ads.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/ads")
    //negatives.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/negatives")

    sc.close()
  }
}
