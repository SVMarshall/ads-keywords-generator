package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import common.implicits.DateTimeFormatters._
import deepmarketing.domain.Keyword
import org.joda.time.DateTime


object AdwordsUploaderPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    //val bq: BigQueryClient = BigQueryClient.defaultInstance()
    val date = DateTime.now()
    val execPath = s"gs://adwords-dataflow/structure-uploader/exec/${date.toTimestamp}"

    // read from structure generation
    val genStructureExecPath = s"gs://adwords-dataflow/structure-generator/exec/${args("generatedSructureTimestamp")}"
    val genStructureKeywords = sc.objectFile[Keyword](s"${genStructureExecPath}/keywords")

    // join with accounts state
    // TODO

    // output upload job

    //val inputFacets: SCollection[Seq[InputFacet]] = InputFacetsRepository.getInputFacets(sc, bq)
    //val keywords: SCollection[Keyword] = KeywordService.generateKeywordsFromInputFacets(inputFacets)
    //val ads: SCollection[Ad] = AdService.generateAds(keywords)
    //val negatives: SCollection[Negative] = NegativeService.generateNegatives(keywords)

    //keywords.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/keywords")
    //ads.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/ads")
    //negatives.map(_.csvEncode()).saveAsTextFile("gs://adwords-dataflow/negatives")

    CampaignsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/campaigns")
    AdgroupsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/adgroups")
    KeywordsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/adgroups")

    sc.close()
  }

  case class CampaignAdwordsFields(accountName: String, campaignState: String, campaign: String, budget: String,
                                   status: String, campaignType: String, campaignSubtype: String, bidStrategyType: String,
                                   location: String, deliveryMethod: String, targetingMethod: String, exclusionMethod: String,
                                   language: String, adRotation: String) {
    override def toString: String =
      Seq(accountName, campaignState, campaign, budget, status, campaignType, campaignSubtype, bidStrategyType,
          location, deliveryMethod, targetingMethod, exclusionMethod, language, adRotation)
        .map("\"" + _ + "\"").mkString(",")
  }

  object CampaignsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign state", "campaign", "budget", "status", "campaign type",
                     "campaign subtype", "bid strategy type", "location", "delivery method", "targeting method",
                     "exclusion method", "language", "ad rotation")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        CampaignAdwordsFields(accountName = "account1",
                              campaignState = "disabled",
                              campaign = kw.campaignName,
                              budget = "1000",
                              status = "Elegible",
                              campaignSubtype = "All features",
                              campaignType = "Search Only",
                              bidStrategyType = "cpc",
                              location = "Spain",
                              deliveryMethod = "Accelerated",
                              targetingMethod = "Location of presence or Area of interest",
                              exclusionMethod = "Location of presence",
                              language = "ca;en;es",
                              adRotation = "Optimize for clicks").toString}).distinct
    }
  }

  case class AdgroupAdwordsFields(accountName: String, campaign: String, adgroup: String,
                                  adgroupState: String, defaultMaxCpc: String) {
    override def toString: String =
      Seq(accountName, campaign, adgroup, adgroupState, defaultMaxCpc)
        .map("\"" + _ + "\"").mkString(",")
  }

  object AdgroupsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign", "ad group", "ad group state", "default max. cpc")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        AdgroupAdwordsFields(accountName = "account1",
                             campaign = kw.campaignName,
                             adgroup = kw.adGroupName,
                             adgroupState = "disabled",
                             defaultMaxCpc = "0.1").toString}).distinct
    }
  }

  case class KeywordAdwordsFields(accountName: String, campaign: String, adgroup: String,
                                  adgroupState: String, keywordMatchType: String, maxCpc: String) {
    override def toString: String =
      Seq(accountName, campaign, adgroup, adgroupState, keywordMatchType, maxCpc)
        .map("\"" + _ + "\"").mkString(",")
    // keyword state	campaign	ad group	ad group state	keyword	match type	max. cpc
  }

  object KeywordsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign", "ad group", "ad group state", "keyword match type", "max. cpc")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        KeywordAdwordsFields(accountName = "account1",
                             campaign = kw.campaignName,
                             adgroup = kw.adGroupName,
                             adgroupState = "disabled",
                             keywordMatchType = kw.matchType.text,
                             maxCpc = "0.1").toString}).distinct
    }
  }

}