package deepmarketing

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import deepmarketing.negatives.Negatives

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

case class Keyword(name: String, matchType: String, urlLanding: String)

object KeywordsPipeline {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val keywords: SCollection[Keyword] = null  // sc.bigquery bla bla TODO: get from bquery

    val negatives = Negatives.transform(keywords)

    negatives.saveAsAvroFile("negativesPath")

  }


  // Word Count:
  /*
  val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
  val input = args.getOrElse("input", exampleData)
  val output = args("output")

  sc.textFile(input)
    .map(_.trim)
    .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    .countByValue
    .map(t => t._1 + ": " + t._2)
    .saveAsTextFile(output)

  val result = sc.close().waitUntilFinish()
  */
}
