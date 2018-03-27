package deepmarketing

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.BigQueryClient
import deepmarketing.structure.{Adgroup, BaseAdgroups, Negatives, Negative}

/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/


object KeywordsPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val baseAdgroups: SCollection[Adgroup] = BaseAdgroups.get()

    val negatives = SCollection[Negative] = Negatives.get(baseAdgroups)
  }

}
