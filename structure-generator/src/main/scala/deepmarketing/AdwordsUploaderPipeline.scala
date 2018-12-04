package deepmarketing

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import common.implicits.DateTimeFormatters._
import deepmarketing.domain.{Ad, Keyword}
import deepmarketing.utils.cue.lang.stop.StopWords
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.MINUTES


object AdwordsUploaderPipeline {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val date = DateTime.now()
    val execPath = s"gs://adwords-dataflow/structure-uploader/exec/${date.toTimestamp}"

    val generatedStructureTimestamp = args("generatedSructureTimestamp")
    log.info(s"generatedSructureTimestamp = $generatedStructureTimestamp")

    // read from structure generation
    val genStructureExecPath = s"gs://adwords-dataflow/structure-generator/exec/$generatedStructureTimestamp"
    val genStructureKeywords = sc.objectFile[Keyword](s"$genStructureExecPath/keywords/*")
      .withName("generatedKeywords_" + generatedStructureTimestamp)

    val genStructureAds = sc.objectFile[Ad](s"$genStructureExecPath/ads/*")
      .withName("generatedAds_" + generatedStructureTimestamp)

    CampaignsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"$execPath/campaigns")
    KeywordsAdwordsFieldsBuilder.getFields(genStructureKeywords).saveAsTextFile(s"$execPath/keywords")
    AdsAdwordsFieldsBuilder.getFields(genStructureAds).saveAsTextFile(s"$execPath/ads")

    sc.close().waitUntilDone(60, MINUTES)
    log.info("Pipeline finished")

    // marge files to get a single csv; temporary workaround until we upload changes through Adwords API
    def composeCsv(timestamp: String, structureField : String) = {
      import sys.process._
      val headersFile = s"gs://adwords-dataflow/structure-uploader/headers/${structureField}_header.txt"
      val structureFiles = s"gs://adwords-dataflow/structure-uploader/exec/${timestamp}/${structureField}s/*"
      val structureOutput = s"gs://adwords-dataflow/structure-uploader/csv/${timestamp}/${structureField}s.csv"
      val composeCommand = "gsutil compose " + headersFile + " " + structureFiles + " " + structureOutput

      println("Compose command: " + composeCommand)
      val composeExec = composeCommand.!

      println("Compose Command output:" + composeExec)
    }
    composeCsv(date.toTimestamp, "campaign")
    composeCsv(date.toTimestamp, "adgroup")
    composeCsv(date.toTimestamp, "ad")
    composeCsv(date.toTimestamp, "keyword")
  }

  case class CampaignAdwordsFields(accountName: String, campaignState: String, campaign: String, budget: String,
                                   campaignType: String, campaignSubtype: String, bidStrategyType: String, locationId: String,
                                   location: String, deliveryMethod: String, targetingMethod: String, exclusionMethod: String,
                                   language: String, adRotation: String) {
    override def toString: String =
      Seq(/*accountName, */ campaignState, campaign, budget, campaignType, campaignSubtype, bidStrategyType, locationId,
          location, deliveryMethod, targetingMethod, exclusionMethod, language, adRotation)
        .map("\"" + _ + "\"").mkString(",")
  }

  object CampaignsAdwordsFieldsBuilder {
    val header = Seq("account name", "campaign state", "campaign", "budget", "campaign type",
      "campaign subtype", "bid strategy type", "location id", "location", "delivery method", "targeting method",
                     "exclusion method", "language", "ad rotation")
    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        CampaignAdwordsFields(accountName = "account1",
                              campaignState = "disabled",
                              campaign = kw.campaignName,
                              budget = "1000",
                              campaignSubtype = "All features",
                              campaignType = "Search Only",
                              bidStrategyType = "cpc",
          locationId = "2724",
                              location = "Spain",
                              deliveryMethod = "Accelerated",
                              targetingMethod = "Location of presence or Area of interest",
                              exclusionMethod = "Location of presence",
                              language = "ca;en;es",
                              adRotation = "Optimize for clicks").toString}).withName("createKeywordCampaign")
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
    val header = Seq("account name", "campaign", "ad group", "ad group state", "keyword", "match type", "max cpc")

    def removePunctuation(criteria: String) = {
      criteria
        .replaceAll("á|à", "a")
        .replaceAll("é|è", "e")
        .replaceAll("í", "i")
        .replaceAll("ó|ò", "o")
        .replaceAll("ú", "u")
    }

    def getFields(keywords: SCollection[Keyword]): SCollection[String] = {
      keywords.map(kw => {
        // get kw text without punctuation
        val kwText = removePunctuation(kw.criteria)

        KeywordAdwordsFields(accountName = "account1",
                             campaign = kw.campaignName,
                             adgroup = kw.adGroupName,
          adgroupState = "enabled",
          keyword =
            if (kw.matchType.text == "BROAD") {
              kwText.split(" ").map(w => {
                if (!StopWords.Spanish.isStopWord(w)) "+" + w else w}
              ).mkString(" ")
            } else {
              kwText
            },
          matchType = kw.matchType.text,
                             maxCpc = "0.01",
          keywordState = "enabled").toString
      }).withName("createKeyword")
        .distinct
    }
  }

  case class AdsAdwordsFields(adgroup: String,
                              headline1: String, headline2: String, description: String,
                              path1: String, path2: String, finalUrl: String) {
    override def toString: String =
      Seq(/*accountName, */adgroup, headline1, headline2, description, path1, path2, finalUrl)
        .map("\"" + _ + "\"").mkString(",")
  }

  object AdsAdwordsFieldsBuilder {
    val header = Seq("ad group", "headline 1",
                     "headline 2", "description", "path 1", "path 2", "final url")

    def getFields(ads: SCollection[Ad]): SCollection[String] = {
      ads.map(ad => {
          AdsAdwordsFields(
            adgroup = ad.adGroupName,
            headline1 = ad.h1,
            headline2 = ad.h2,
            description = ad.description,
            path1 = "null",
            path2 = "null",
            finalUrl = ad.finalUrl
          ).toString
      }).withName("createKeywordAds")
        .distinct
    }
  }

}