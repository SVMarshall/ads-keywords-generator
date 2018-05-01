package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Ad, AdTemplate, Keyword}
import deepmarketing.infrastructure.repositories.AdTemplatesRepository


object AdService {

  def generateAds(keywords: SCollection[Keyword]): SCollection[Ad] = {
    keywords.flatMap(keyword => generateAdsForKeyword(keyword))
  }

  private def generateAdsForKeyword(keyword: Keyword): Seq[Ad] = {

    val adTemplates: Seq[AdTemplate] = AdTemplatesRepository.getAdTemplatesForKeyword(keyword)

    adTemplates.map(template => {
      Ad(
        template.replaceTagsInH1(keyword.getInputFacets),
        template.replaceTagsInH2(keyword.getInputFacets),
        template.replaceTagsInDescription(keyword.getInputFacets),
        template.replaceTagsInUrl1(keyword.getInputFacets),
        template.replaceTagsInUrl2(keyword.getInputFacets),
        keyword.adGroupName,
        "finalUrls TODO"  //TODO
      )
    })
  }
}
