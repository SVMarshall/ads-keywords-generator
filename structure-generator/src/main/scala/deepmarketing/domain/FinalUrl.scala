package deepmarketing.domain


case class FinalUrl(keyword: Keyword) {

  def composeUrl: String = {
    val baseUrl: String = "https://www.trovimap.com/Compra/Vivienda/Barcelona/Barcelona" // TODO: Extract baseUrl from client config spreadsheet
    addInputFacetsToUrl(addGeoToUrl(baseUrl))
  }

  private def addGeoToUrl(baseUrl: String): String = {
    if (keyword.getGeo.isEmpty) {
      baseUrl + "?"
    } else {
      baseUrl + "/" + keyword.getGeo.replaceAll(" ", "-") + ",Barcelona"
    }
  }

  private def addInputFacetsToUrl(baseUrl: String): String = {
    keyword.getInputFacets.map(inputFacet => {inputFacet.url_name + "=" + inputFacet.url_value}).mkString("&")
  }
}