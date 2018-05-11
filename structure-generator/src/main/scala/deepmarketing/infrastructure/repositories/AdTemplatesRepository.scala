package deepmarketing.infrastructure.repositories

import deepmarketing.domain.{AdTemplate, InputFacet, Keyword}

object AdTemplatesRepository {


  def getAdTemplatesForKeyword(keyword: Keyword,
                               adTemplatesValues: List[List[String]],
                               header: List[String]): Seq[AdTemplate] = {

    val inputFacets: Seq[InputFacet] = keyword.getInputFacets

    adTemplatesValues
      // Filter the templates that contain the right input facets
      .filter(_.head.split("\\+").map(_.trim.toLowerCase).toSet == inputFacets.map(_.facet).toSet)
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
