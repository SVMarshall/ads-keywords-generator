package deepmarketing.domain

import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet

//object Keyword {
//@AvroType.toSchema
case class Keyword(inputFacets: Seq[InputFacet],
                   criteria: String,
                   matchType: MatchType,
                   adGroupName: String,
                   campaignName: String,
                   ads: Seq[Ad] = Seq()) {
  def addAds(ads: Seq[Ad]): Keyword = this.copy(ads = ads)

  def csvEncode: String = Seq(criteria, matchType.text, campaignName, adGroupName).map("\"" + _ + "\"").mkString(",")

  def getInputFacets: Seq[InputFacet] = {
    this.inputFacets.filter(_.field.get != "none")
  }

  def getGeo: String = {
    val geo: String = this.inputFacets.filter(_.facet.get == "geo").head.field.get
    if (geo == "none") "" else geo
  }
}

//}
