package deepmarketing.domain

case class AdGroup(name: String, urlLanding: String, keyword: Keyword, negatives: Seq[Negative])
