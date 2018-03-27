package deepmarketing.structure.transformers

import com.spotify.scio.values.SCollection

trait Transformer {
  /**
    * Transform the dataset
    *
    * @param dataset input DataFrame
    * @return transformed DataFrame
    */
  def transform(dataset: SCollection[Any]): SCollection[Any]
}
