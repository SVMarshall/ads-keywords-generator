package deepmarketing.services

import com.spotify.scio.values.SCollection
import common.spreadsheet.SheetsBuilder
import deepmarketing.domain.{Ad, Keyword}

object AdService {
  def generateAds(keywords: SCollection[Keyword]): SCollection[Ad] = {

    val adConfigs: List[List[String]] =
      SheetsBuilder.build("1bNG0OfnuCMSiJVnMfZssCUEJlQXWPphUKCovedFcTpE").getValues("ad_config", "A2:F1000000")
    keywords.flatMap(keyword => generateAdsFromKeyword(keyword, adConfigs))

  }

  private def generateAdsFromKeyword(keyword: Keyword, adConfigs: List[List[String]]): Seq[Ad] = {
    val geo: String = keyword.inputFacets.filter(_.facet.get == "geo").head.field.getOrElse("")
    val propertyType: String = keyword.inputFacets.filter(_.facet.get == "type").head.field.getOrElse("")
    val action: String = keyword.inputFacets.filter(_.facet.get == "action").head.field.getOrElse("")
    val rooms: String = keyword.inputFacets.filter(_.facet.get == "rooms").head.field.getOrElse("")


    val configId: String = action + "+" + propertyType + "+" + geo + "+" + rooms

    adConfigs.filter(_.head == configId).map(config => {
      config.tail.map(_.replaceAllLiterally("#ACTION#", action)
        .replaceAllLiterally("#TYPE#", propertyType)
        .replaceAllLiterally("#ROOMS#", rooms)
        .replaceAllLiterally("#GEO#", geo))
    }).map(config => {
      Ad(config.head, config(1), config(2), config(3), config(4), keyword.adGroupName)}
    )
  }
}
