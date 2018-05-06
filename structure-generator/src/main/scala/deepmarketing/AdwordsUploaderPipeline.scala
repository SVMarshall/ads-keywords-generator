package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import common.implicits.DateTimeFormatters._
import deepmarketing.KeywordsPipeline.log
import deepmarketing.domain.Keyword
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.duration.MINUTES


object AdwordsUploaderPipeline {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    //val bq: BigQueryClient = BigQueryClient.defaultInstance()
    val date = DateTime.now()
    val execPath = s"gs://adwords-dataflow/structure-uploader/exec/${date.toTimestamp}"

    val generatedSructureTimestamp = args("generatedSructureTimestamp")
    //val generatedSructureTimestamp = "20180501174244"
    log.info(s"generatedSructureTimestamp = ${generatedSructureTimestamp}")

    // read from structure generation
    // val genStructureExecPath = "gs://adwords-dataflow/structure-generator/exec/20180501174244"
    val genStructureExecPath = s"gs://adwords-dataflow/structure-generator/exec/${generatedSructureTimestamp}"
    val genStructureKeywords = sc.objectFile[Keyword](s"${genStructureExecPath}/keywords/*")
      .withName("generatedKeywords_" + generatedSructureTimestamp)

    CampaignsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/campaigns")
    AdgroupsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/adgroups")
    KeywordsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/keywords")
    AdsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"${execPath}/ads")

    sc.close().waitUntilDone(60, MINUTES)
    log.info("Pipeline finished")

    // marge files to get a single csv; temporary workaround until we upload changes through Adwords API
    def composeCsv(timestamp: String, structureField : String) = {
      import sys.process._
      val headersFile = s"gs://adwords-dataflow/structure-uploader/headers/${structureField}_header.txt"
      val structureFiles = s"gs://adwords-dataflow/structure-uploader/exec/${timestamp}/${structureField}s/*"
      val structureOutput = s"gs://adwords-dataflow/structure-uploader/csv/${timestamp}/${structureField}s.csv"
      val composeCommand = "gsutil compose " + headersFile + " " + structureFiles + " " + structureOutput
      //log.info("Compose command: " + composeCommand)
      println("Compose command: " + composeCommand)
      val composeExec = composeCommand.!
      //log.info("Compose Command output:" + composeExec)
      println("Compose Command output:" + composeExec)
    }
    composeCsv(date.toTimestamp, "campaign")
    composeCsv(date.toTimestamp, "adgroup")
    composeCsv(date.toTimestamp, "ad")
    composeCsv(date.toTimestamp, "keyword")
  }

  case class CampaignAdwordsFields(accountName: String, campaignState: String, campaign: String, budget: String,
                                   status: String, campaignType: String, campaignSubtype: String, bidStrategyType: String,
                                   location: String, deliveryMethod: String, targetingMethod: String, exclusionMethod: String,
                                   language: String, adRotation: String) {
    override def toString: String =
      Seq(/*accountName, */campaignState, campaign, budget, status, campaignType, campaignSubtype, bidStrategyType,
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
                              adRotation = "Optimize for clicks").toString}).withName("createKeywordCampaign")
        .distinct
    }
  }

  case class AdgroupAdwordsFields(accountName: String, campaign: String, adgroup: String,
                                  adgroupState: String, defaultMaxCpc: String) {
    override def toString: String =
      Seq(/*accountName, */campaign, adgroup, adgroupState, defaultMaxCpc)
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
                             defaultMaxCpc = "0.01").toString}).withName("createKeywordAdgroup")
        .distinct
    }
  }

  case class KeywordAdwordsFields(accountName: String, campaign: String, adgroup: String, adgroupState: String,
                                  keyword: String, matchType: String, maxCpc: String, keywordState: String) {
    override def toString: String =
      Seq(/*accountName, campaign, */keywordState, campaign, adgroup, adgroupState, keyword, matchType, maxCpc)
        .map("\"" + _ + "\"").mkString(",")
  }

  object KeywordsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign", "ad group", "ad group state", "keyword", "match type", "max. cpc")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        KeywordAdwordsFields(accountName = "account1",
                             campaign = kw.campaignName,
                             adgroup = kw.adGroupName,
                             adgroupState = "disabled",
                             keyword = kw.criteria,
                             matchType = kw.matchType.text.capitalize,
                             maxCpc = "0.01",
                             keywordState = "disabled").toString}).withName("createKeyword")
        .distinct
    }
  }

  case class AdsAdwordsFields(accountName: String, campaign: String, adgroup: String,
                              headline1: String, headline2: String, description: String,
                              path1: String, path2: String, finalUrl: String) {
    override def toString: String =
      Seq(/*accountName, */campaign, adgroup, headline1, headline2, description, path1, path2, finalUrl)
        .map("\"" + _ + "\"").mkString(",")
  }

  object AdsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign", "ad group", "headline 1",
                     "headline 2", "description", "path 1", "path 2", "final url")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.flatMap(kw => {
        kw.ads.map(ad => {
          AdsAdwordsFields(accountName = "account1",
                           campaign = kw.campaignName,
                           adgroup = kw.adGroupName,
                           headline1 = ad.h1,
                           headline2 = ad.h2,
                           description = ad.description,
                           path1 = "null",
                           path2 = "null",
                           finalUrl = ad.finalUrl).toString})
      }).withName("createKeywordAds")
        .distinct
    }
  }

}