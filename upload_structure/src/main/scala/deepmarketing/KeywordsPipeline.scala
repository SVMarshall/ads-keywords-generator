package deepmarketing

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.BigQueryClient
import deepmarketing.infrastructure.repositories._
import deepmarketing.domain.{Negative, AdGroup}


/*
sbt "runMain [PACKAGE].KeywordsPipeline
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object KeywordsPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)

    val bq: BigQueryClient = BigQueryClient.defaultInstance()

    val baseAdGroups: SCollection[AdGroup] =
      AdGroupRepository.generateAdGroupsFromInputFacets(InputFacetsRepository.getInputFacets(sc, bq))
    //val baseAdGroups = AdGroupRepository.generateTestAdGroups(sc)
    val negatives: SCollection[(String, Seq[Negative])] = NegativeRepository.generateNegatives(baseAdGroups)

    negatives.saveAsTextFile("gs://adwords-dataflow/keywords")
    sc.close()

  }
}
