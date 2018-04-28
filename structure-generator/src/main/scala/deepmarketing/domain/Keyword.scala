package deepmarketing.domain

import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacet

case class Keyword(inputFacets: Seq[InputFacet],
                   criteria: String,
                   matchType: MatchType,
                   adGroupName: String,
                   campaignName: String,
                   ads: Seq[Ad] = Seq()) {
  def addAds(ads: Seq[Ad]): Keyword = {
    this.copy(ads = ads)
  }
}
