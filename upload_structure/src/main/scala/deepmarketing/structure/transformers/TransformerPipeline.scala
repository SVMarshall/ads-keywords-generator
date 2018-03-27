package deepmarketing.structure.transformers

import com.spotify.scio.values.SCollection

import scala.collection.mutable.ListBuffer

class TransformerPipeline {
  val transformers: ListBuffer[Transformer] = collection.mutable.ListBuffer[Transformer]()

  def addStage(transformer: Transformer): TransformerPipeline = {
    transformers += transformer
    this
  }

  def setStages(transformers: Transformer*): TransformerPipeline = {
    this.transformers.clear
    this.transformers ++= transformers
    this
  }

  def fit(dataset: SCollection[Any]): SCollection[Any] = {
    transformers.foldLeft(dataset)((ds, transformer) => {
      transformer.transform(dataset)
    })
  }
}
