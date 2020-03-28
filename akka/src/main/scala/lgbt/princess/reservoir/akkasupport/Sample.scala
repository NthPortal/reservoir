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
   * @return a stream operator that randomly samples elements of the stream
   */
  def apply[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean = false)(
      map: A => B,
  ): Flow[A, A, Future[IndexedSeq[B]]] = {
    Sampler.validateParams(maxSampleSize)
    Flow[A].viaMat(new SampleImpl[A, B](Sampler(maxSampleSize, preAllocate)(map)))(Keep.right)
  }
}
