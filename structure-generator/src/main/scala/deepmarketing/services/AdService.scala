package deepmarketing.services

import com.spotify.scio.values.SCollection
import common.spreadsheet.SheetsBuilder
import deepmarketing.domain.{Ad, Keyword}

object AdService {
  def generateAds(keywords: SCollection[Keyword]): SCollection[Ad] = {
    var adConfigs: List[List[String]] =
      SheetsBuilder.build("1bNG0OfnuCMSiJVnMfZssCUEJlQXWPphUKCovedFcTpE").getValues("ad_config", "A2:F1000000")
    keywords.flatMap(keyword => generateAdsFromKeyword(keyword))
  }

  private def generateAdsFromKeyword(keyword: Keyword, adConfigs: List[List[String]]): Seq[Ad] = {
    val geo: String = keyword.inputFacets.filter(_.facet.get == "geo").head.field.getOrElse("")
    val propertyType: String = keyword.inputFacets.filter(_.facet.get == "type").head.field.getOrElse("")
    val action: String = keyword.inputFacets.filter(_.facet.get == "action").head.field.getOrElse("")
    val rooms: String = keyword.inputFacets.filter(_.facet.get == "rooms").head.field.getOrElse("")

    var configId: String = action + "+" + propertyType + "+" + geo + "+" + rooms
    adConfigs.filter(_.head == configId).map(config => {
      config(1).replaceAll("\#(.*?)\#")
    })


    Seq(
      Ad(generateH1(action, propertyType, geo),
        generateH2(),
        generateDescription(rooms, propertyType, geo),
        generateUrlDisplay(propertyType),
        keyword.adGroupName)
    )
  }

  private def generateH1(action: Any, propertyType: Any, geo: Any): String = {
    s"$action $propertyType en $geo"
  }

  private def generateH2(): String = {
    "#Generic#"
  }

  private def generateDescription(rooms: Any, propertyType: Any, geo: Any): String = {
    s"#Generic# $propertyType en $geo $rooms"
  }

  private def generateUrlDisplay(propertyType: Any): String = {
    s"#ClientUrl#/$propertyType/Barcelona"
  }
}
