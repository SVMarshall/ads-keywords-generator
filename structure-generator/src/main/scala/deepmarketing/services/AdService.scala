package deepmarketing.services

import com.spotify.scio.values.SCollection
import common.spreadsheet.SheetsBuilder
import deepmarketing.domain.{Ad, AdTemplate, FinalUrl, Keyword}
import deepmarketing.infrastructure.repositories.AdTemplatesRepository


object AdService {

  def generateAds(keywords: SCollection[Keyword], clientConfigSheet: String): SCollection[Ad] = {

    val adTemplatesValues: List[List[String]] =
      SheetsBuilder.build(clientConfigSheet).getValues("ad_config", "A2:F1000000")

    val header: List[String] =
      SheetsBuilder.build(clientConfigSheet: String).getValues("ad_config", "A1:F1").head

    keywords.flatMap(keyword => generateAdsForKeyword(keyword, adTemplatesValues, header))
  }

  private def generateAdsForKeyword(keyword: Keyword,
                                    adTemplatesValues: List[List[String]],
                                    header: List[String]): Seq[Ad] = {

    val adTemplates: Seq[AdTemplate] =
      AdTemplatesRepository.getAdTemplatesForKeyword(keyword, adTemplatesValues, header)
    adTemplates.map(template => {
      Ad(
        template.replaceTagsInH1(keyword.getInputFacets),
        template.replaceTagsInH2(keyword.getInputFacets),
        template.replaceTagsInDescription(keyword.getInputFacets),
        template.replaceTagsInUrl1(keyword.getInputFacets),
        template.replaceTagsInUrl2(keyword.getInputFacets),
        keyword.adGroupName,
        FinalUrl(keyword).composeUrl
      )
    })
  }


}
