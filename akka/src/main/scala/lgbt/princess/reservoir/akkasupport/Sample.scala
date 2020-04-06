package lgbt.princess.reservoir
package akkasupport

import akka.stream.scaladsl.{Flow, Keep}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Utility for randomly sampling elements from a stream, without keeping
 * any elements other than those sampled in memory; this is known as
 * [[https://en.wikipedia.org/wiki/Reservoir_sampling reservoir sampling]].
 *
 * '''Emits when''' upstream pushes and an element is possibly sampled
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' downstream cancels
 */
object Sample {

  private def flow[A, B](newSampler: => Sampler[A, B]): Flow[A, A, Future[IndexedSeq[B]]] =
    Flow[A].viaMat(new SampleImpl[A, B](newSampler))(Keep.right)

  /**
   * Creates a stream operator that samples elements with equal probability,
   * and allows duplicate elements in the sample.
   *
   * @param maxSampleSize the maximum number of elements to sample
   * @param preAllocate   whether or not to pre-allocate space for the
   *                      maximum number of sampled elements
   * @param map           a mapping function to apply to elements being sampled;
   *                      this may be called more than `maxSampleSize` times
   * @tparam A the type of elements being sampled from
   * @tparam B the type of sample elements being stored
   * @throws scala.IllegalArgumentException if `maxSampleSize` is negative or exceeds VM limit
   * @throws scala.NullPointerException     if `map` is `null`
   * @return a stream operator that randomly samples elements of the stream
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def apply[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean = false)(
      map: A => B,
  ): Flow[A, A, Future[IndexedSeq[B]]] = {
    Sampler.validateNonDistinctParams(maxSampleSize, map)
    flow(Sampler(maxSampleSize, preAllocate)(map))
  }

  /**
   * Creates a stream operator that samples distinct values with equal probability;
   * it does not allow duplicate elements in the sample.
   *
   * @note Instances returned by this method are less efficient both in memory and
   *       CPU than those returned by [[apply]], due to the need to sample distinct
   *       elements.
   *
   * @param maxSampleSize the maximum number of elements to keep in the sample;
   *                      if at least this many elements are sampled, this will
   *                      be the size of the final sample
   * @param map           a mapping function to apply to elements being sampled;
   *                      it is called for each element sampled
   * @param hash          a function used to hash elements of the sample. By default,
   *                      `B#hashCode()` is used, but if `B#hashCode()` does not
   *                      reliably generate different values for different elements,
   *                      a custom hash function should be provided. Additionally,
   *                      if a cheaper or higher-granularity hash function exists,
   *                      that should be used instead.
   * @tparam A the type of elements being sampled from
   * @tparam B the type of sample elements being stored
   * @throws scala.IllegalArgumentException if `maxSampleSize` is negative or exceeds VM limit
   * @throws scala.NullPointerException     if `map` or `hash` is `null`
   * @return a stream operator that randomly samples distinct elements of the stream
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def distinct[A, B: ClassTag](
      maxSampleSize: Int,
  )(map: A => B, hash: B => Long = Sampler.defaultHashFunction): Flow[A, A, Future[IndexedSeq[B]]] = {
    Sampler.validateDistinctParams(maxSampleSize, map, hash)
    flow(Sampler.distinct(maxSampleSize)(map, hash))
  }
}
