package deepmarketing.infrastructure.repositories

import com.spotify.scio.values.SCollection
import deepmarketing.domain.{Negative, AdGroup}

object NegativeRepository {

  def generateNegatives(adGroups: SCollection[AdGroup]): SCollection[(String, Seq[Negative])] = {

    val commonJoinKey: Int = 1
    val adGroupsMap: SCollection[(Int, AdGroup)] =
      adGroups.map((commonJoinKey, _))

    adGroupsMap
      .join(adGroupsMap)
      .flatMap(keyValueAdGroups => {
        val adGroup = keyValueAdGroups._2._1
        val joinAdGroup = keyValueAdGroups._2._2

        if (joinAdGroup.keyword.text.contains(adGroup.keyword.text)) {
          if (joinAdGroup.keyword.text == adGroup.keyword.text) {
            if (joinAdGroup.keyword.matchType > adGroup.keyword.matchType) {
              Seq((adGroup.name, joinAdGroup.keyword.toNegative))
            } else {
              Seq()
            }
          } else {
            if (joinAdGroup.keyword.matchType.text != "BRD"
              & joinAdGroup.keyword.matchType.isGreaterOrEqual(adGroup.keyword.matchType)) {
              Seq((adGroup.name, joinAdGroup.keyword.toNegative))
            } else {
              Seq()
            }
          }
        } else {
          Seq()
        }
      }).aggregateByKey(Seq[Negative]())(_ :+ _, _ ++ _)
  }
}

// bcn casa | phr
// comprar bcn casa 3 hab | brd      bcn casa ext
// bcn casa piscina
// bcn casa piscina balcon
// bcn casa piscina atico
// bcn casa piscina caca


