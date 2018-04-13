package deepmarketing.domain

case class Campaign(name: String, urlLanding: String, keyword: Keyword, negatives: Seq[Negative])
