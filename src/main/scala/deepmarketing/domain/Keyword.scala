package deepmarketing.domain

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
    inputFacets.filter(!_.field.isEmpty)
  }

  def getGeo: String = {
    inputFacets.filter(_.facet == "geo").head.field
  }
}

//}
