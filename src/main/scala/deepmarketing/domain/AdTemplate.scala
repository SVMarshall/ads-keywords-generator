package deepmarketing.domain

case class AdTemplate(facetsUsed: String,
                      h1: String,
                      h2: String,
                      description: String,
                      url_display_1: String,
                      url_display_2: String
                     ) {

  def replaceTagsInH1(inputFacets: Seq[InputFacet]): Option[String] = {
    var (replacementFacet, replacementMainFacet) = (h1, h1)
    inputFacets.foreach(inputFacet => {
      replacementFacet = replacementFacet.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
      replacementMainFacet = replacementMainFacet.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
    })

    Some(
      Some(replacementFacet)
        .filter(isValidHeadline)
        .getOrElse(replacementMainFacet)
    ).filter(isValidHeadline)
  }

  def replaceTagsInH2(inputFacets: Seq[InputFacet]): Option[String] = {
    var (replacementFacet, replacementMainFacet) = (h2, h2)
    inputFacets.foreach(inputFacet => {
      replacementFacet = replacementFacet.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
      replacementMainFacet = replacementMainFacet.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
    })

    Some(
      Some(replacementFacet)
        .filter(isValidHeadline)
        .getOrElse(replacementMainFacet)
    ).filter(isValidHeadline)
  }

  def replaceTagsInDescription(inputFacets: Seq[InputFacet]): Option[String] = {
    var (replacementFacet, replacementMainFacet) = (description, description)
    inputFacets.foreach(inputFacet => {
      replacementFacet = replacementFacet.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
      replacementMainFacet = replacementMainFacet.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
    })

    Some(
      Some(replacementFacet)
        .filter(isValidDescription)
        .getOrElse(replacementMainFacet)
    ).filter(isValidDescription)
  }

  def replaceTagsInUrl1(inputFacets: Seq[InputFacet]): Option[String] = {
    var (replacementFacet, replacementMainFacet) = (url_display_1, url_display_1)
    inputFacets.foreach(inputFacet => {
      replacementFacet = replacementFacet.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
      replacementMainFacet = replacementMainFacet.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
    })

    Some(
      Some(replacementFacet)
        .filter(isValidUrl)
        .getOrElse(replacementMainFacet)
    ).filter(isValidUrl)
  }

  def replaceTagsInUrl2(inputFacets: Seq[InputFacet]): Option[String] = {
    var (replacementFacet, replacementMainFacet) = (url_display_2, url_display_2)
    inputFacets.foreach(inputFacet => {
      replacementFacet = replacementFacet.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
      replacementMainFacet = replacementMainFacet.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
    })

    Some(
      Some(replacementFacet)
        .filter(isValidUrl)
        .getOrElse(replacementMainFacet)
    ).filter(isValidUrl)
  }

  private def useMainFacetIfDefined(inputFacet: InputFacet): String = {
    if (inputFacet.main_facet.isEmpty) inputFacet.field else inputFacet.main_facet
  }

  private def isValidHeadline(text: String) = {
    text.length <= 25
  }

  private def isValidDescription(text: String) = {
    text.length <= 80
  }

  private def isValidUrl(text: String) = {
    text.length <= 15
  }
}
