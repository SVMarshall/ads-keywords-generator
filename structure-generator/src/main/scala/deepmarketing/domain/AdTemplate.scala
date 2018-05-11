package deepmarketing.domain

case class AdTemplate(facetsUsed: String,
                      h1: String,
                      h2: String,
                      description: String,
                      url_display_1: String,
                      url_display_2: String
                     ) {

  def replaceTagsInH1(inputFacets: Seq[InputFacet]): String = {
    var replacement: String = h1
    inputFacets.foreach(inputFacet => {
      replacement = replacement.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
    })

    if (isValidHeadline(replacement)) replacement else {
      replacement = h1
      inputFacets.foreach(inputFacet => {
        replacement = replacement.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
      })
      replacement
    }
  }

  def replaceTagsInH2(inputFacets: Seq[InputFacet]): String = {
    var replacement: String = h2
    inputFacets.foreach(inputFacet => {
      replacement = replacement.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
    })

    if (isValidHeadline(replacement)) replacement else {
      replacement = h2
      inputFacets.foreach(inputFacet => {
        replacement = replacement.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
      })
      replacement
    }
  }

  def replaceTagsInDescription(inputFacets: Seq[InputFacet]): String = {
    var replacement: String = description
    inputFacets.foreach(inputFacet => {
      replacement = replacement.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
    })

    if (isValidDescription(replacement)) replacement else {
      replacement = description
      inputFacets.foreach(inputFacet => {
        replacement = replacement.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
      })
      replacement
    }
  }

  def replaceTagsInUrl1(inputFacets: Seq[InputFacet]): String = {
    var replacement: String = url_display_1
    inputFacets.foreach(inputFacet => {
      replacement = replacement.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
    })

    if (isValidUrl(replacement)) replacement else {
      replacement = url_display_1
      inputFacets.foreach(inputFacet => {
        replacement = replacement.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
      })
      replacement
    }
  }

  def replaceTagsInUrl2(inputFacets: Seq[InputFacet]): String = {
    var replacement: String = url_display_2
    inputFacets.foreach(inputFacet => {
      replacement = replacement.replaceAll(s"#${inputFacet.facet}#", inputFacet.field).trim
    })

    if (isValidUrl(replacement)) replacement else {
      replacement = url_display_2
      inputFacets.foreach(inputFacet => {
        replacement = replacement.replaceAll(s"#${inputFacet.facet}#", useMainFacetIfDefined(inputFacet)).trim
      })
      replacement
    }
  }

  private def useMainFacetIfDefined(inputFacet: InputFacet): String = {
    if (inputFacet.main_facet.isEmpty) inputFacet.main_facet else inputFacet.field
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
