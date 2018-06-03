package deepmarketing.services

import com.spotify.scio.values.SCollection
import deepmarketing.common.spreadsheet.SheetsBuilder
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
      template.replaceTagsInH1(keyword.getInputFacets)
        .flatMap(h1 => {
          template.replaceTagsInH2(keyword.getInputFacets)
            .flatMap(h2 => {
              template.replaceTagsInDescription(keyword.getInputFacets)
                .flatMap(description => {
                  template.replaceTagsInUrl1(keyword.getInputFacets)
                    .flatMap(url1 => {
                      template.replaceTagsInUrl2(keyword.getInputFacets)
                        .flatMap(url2 => {
                          Option(
                            Ad(h1,
                              h2,
                              description,
                              url1,
                              url2,
                              keyword.adGroupName,
                              FinalUrl(keyword).composeUrl)
                          )
                        })
                    })
                })
            })
        })
    }).filter(_.isDefined).map(_.get)
  }
}
