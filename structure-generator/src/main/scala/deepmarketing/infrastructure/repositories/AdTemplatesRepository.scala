package deepmarketing.infrastructure.repositories

import common.spreadsheet.SheetsBuilder
import deepmarketing.domain.{AdTemplate, Keyword}


object AdTemplatesRepository {

  lazy val adTemplatesValues: List[List[String]] =
    SheetsBuilder.build("1bNG0OfnuCMSiJVnMfZssCUEJlQXWPphUKCovedFcTpE").getValues("ad_config", "A2:F1000000")

  lazy val header: List[String] =
    SheetsBuilder.build("1bNG0OfnuCMSiJVnMfZssCUEJlQXWPphUKCovedFcTpE").getValues("ad_config", "A1:F1").head

  def getAdTemplatesForKeyword(keyword: Keyword): Seq[AdTemplate] = {

    val inputFacets: Seq[InputFacetsRepository.InputFacet] = keyword.getInputFacets

    adTemplatesValues
      // Filter the templates that contain the right input facets
      .filter(_.head.split("\\+").map(_.trim.toLowerCase).toSet == inputFacets.map(_.facet.get).toSet)
      //Create the AdTemplates
      .map(
      adTemplateValues => {
        AdTemplate(
          adTemplateValues(header.indexOf("facets")),
          adTemplateValues(header.indexOf("h1")),
          adTemplateValues(header.indexOf("h2")),
          adTemplateValues(header.indexOf("description")),
          adTemplateValues(header.indexOf("url_display_1")),
          adTemplateValues(header.indexOf("url_display_2")))
      })
  }
}
