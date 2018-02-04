package deepmarketing.negatives

import com.spotify.scio.values.SCollection
import deepmarketing.Keyword

case class Negative(keywordName: String,
                    keywordMatchType: String,
                    negativeName: String,
                    negativeMatchType: String)

object Negatives {
  def transform(keywords: SCollection[Keyword]): SCollection[Negative] = {
    keywords.map(k => Negative(k.name, k.matchType, k.name, k.matchType))
  }
}
