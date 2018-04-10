package deepmarketing.infrastructure.repositories

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import deepmarketing.infrastructure.repositories.InputFacetsRepository.InputFacetsRow

object CampaignsRepository {


  def createBaseCampaigns(sc: ScioContext, facetsMap: Map[Any, List[InputFacetsRow]]): 
  SCollection[Seq[InputFacetsRow]] = {
    sc.parallelize(
      getBaseKeywordOrder().foldRight(Seq[Seq[InputFacetsRow]]())((x, xs) => xs match {
        case Seq() => facetsMap(x).map(Seq(_))
        case _ => xs.flatMap(comb => facetsMap(x).map(_ +: comb))
      })
    )
  }

  // Ho hardcodegem. Hauriem de tenir un standard de fecets. En el cas del primer test la geo es deia zone.
  // El nom del facet que posem ara mateix depen de com es diu a la url.
  private def getBaseKeywordOrder(): Set[String] = {
    Set("action", "propertysubtypegroup", "geo", "minbeds")
  }

  // Per el moment tenim hardcoded com volem la configuració de les campanyes.
  // Més endavant haurem de tenir-ho a bigquery per client
  private def getCampaignCofiguration(): Seq[String] = {
    Seq("action")
  }
}
