package deepmarketing.structure

import com.spotify.scio.values.SCollection
import deepmarketing.Keyword

object Negatives {
  def generateNegatives(adgroups: SCollection[Adgroup]): SCollection[Adgroup] = {
    val matchTypeNegatives: SCollection[Adgroup] = adgroups
        .map(adg => {
          def getMatchTypeNegs(keyword: Keyword): Seq[Negative] = keyword.matchType match {
            case "BRD" => Seq(Negative(keyword.text, "PHR"), Negative(keyword.text, "EXT"))
            case "PHR" => Seq(Negative(keyword.text, "EXT"))
          }
          Adgroup(adg.name, adg.urlLanding, adg.keyword, getMatchTypeNegs(adg.keyword))
        })

    val keyMatchTypeNegatives: SCollection[(Int, Adgroup)] = matchTypeNegatives.map(a => (0, a))

    keyMatchTypeNegatives
      .join(keyMatchTypeNegatives)
      .map(keyVal => {
        val thisAdgroup = keyVal._2._1
        val otherAdgroup = keyVal._2._2
        // if adgroups diferents:
        // 1. si thisAdgr subset of otherAdgr => negativa otherAdgr on this
        Adgroup(thisAdgroup.name,
                thisAdgroup.urlLanding,
                thisAdgroup.keyword,
                thisAdgroup.negatives :+ Negative(null, null))
      })

    // falta fer agregació del mateix adgroup sumar negatives

  }
}

case class Negative(text: String, matchType: String)
case class KeywordNegative(keyword: Keyword, negative: Negative)
