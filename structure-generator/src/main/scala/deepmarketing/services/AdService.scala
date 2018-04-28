package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Ad, Keyword}

object AdService {
  def addAds(keywords: SCollection[Keyword]): SCollection[Keyword] = {
    keywords.map(keyword => keyword.addAds(generateAdsFromKeyword(keyword)))
  }

  private def generateAdsFromKeyword(keyword: Keyword): Seq[Ad] = {
    val geo = keyword.inputFacets.filter(_.facet.get == "geo").head.field.getOrElse("")
    val propertyType = keyword.inputFacets.filter(_.facet.get =="propertysubtypegroup").head.field.getOrElse("")
    val action = keyword.inputFacets.filter(_.facet.get == "action").head.field.getOrElse("")
    val rooms = keyword.inputFacets.filter(_.facet.get == "minbeds").head.field.getOrElse("")

    Seq(Ad(generateH1(action, propertyType, geo),
      generateH2(),
      generateDescription(rooms, propertyType, geo),
      generateUrlDisplay(propertyType)))
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
//
//  Generic #type# en #geo# #tokens# Generic
//  Extensions
//  trovimap/#type#/#geo#
}
