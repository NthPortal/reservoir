package lgbt.princess.reservoir

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Utility for randomly sampling from a stream of elements, without keeping
 * any elements other than those sampled in memory; this is known as
 * [[https://en.wikipedia.org/wiki/Reservoir_sampling reservoir sampling]].
 *
 * @note Instances of this type are NOT reusable unless otherwise specified;
 *       methods other than `isOpen` MUST NOT be invoked after calling
 *       `result()` once.
 * @note Instances of this type are NOT thread-safe unless otherwise specified.
 *
 * @tparam A the type of elements being sampled from
 * @tparam B the type of sample elements being stored
 */
trait Sampler[A, B] {

  /**
   * Sample from another element, probabilistically.
   *
   * For a sample of size `k` and a total of `n` elements sampled, this element
   * has a `k/n` chance of being in the final sample.
   */
  def sample(element: A): Unit

  /**
   * @note methods other than [[isOpen]] (including this one) MUST NOT
   *       be called after calling this method once.
   * @return the sampled elements
   */
  def result(): IndexedSeq[B]

  /**
   * Whether or not this sampler can still sample elements and return
   * a resulting sample.
   */
  def isOpen: Boolean
}

object Sampler {
  private final val MaxSize            = Int.MaxValue - 2 // hotspot VM limit for `Array` size
  private final val DefaultInitialSize = 16
  private final val HalfMax            = 1 << 30 // doubling this gives a negative, which is a pain to work with

  @throws[IllegalArgumentException]
  private[reservoir] def validateParams[A, B](maxSampleSize: Int): Unit = {
    require(maxSampleSize <= MaxSize, "maxSampleSize exceeds VM limit")
    require(maxSampleSize > 0, "maxSampleSize must be positive")
  }

  /**
   * Creates a [[Sampler reservoir sampler]] that samples elements with
   * equal probability, and allows duplicate elements in the sample.
   *
   * @note Instances returned by this method are NOT reusable; methods other than
   *       [[Sampler.isOpen `isOpen`]] MUST NOT be invoked after calling
   *       [[Sampler.result() `result()`]] once.
   * @note Instances returned by this method are NOT thread-safe.
   *
   * @param maxSampleSize the maximum number of elements to keep in the sample;
   *                      if at least this many elements are sampled, this will
   *                      be the size of the final sample
   * @param preAllocate   whether or not to pre-allocate space for the
   *                      maximum number of sampled elements
   * @param map           a mapping function to apply to elements being sampled;
   *                      this may be called more than `maxSampleSize` times
   * @tparam A the type of elements being sampled from
   * @tparam B the type of sample elements being stored
   * @throws scala.IllegalArgumentException if `maxSampleSize` is negative or exceeds VM limit
   * @return an [[Sampler.isOpen open]] reservoir sampler
   */
  @throws[IllegalArgumentException]
  def apply[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean = false)(
      map: A => B,
  ): Sampler[A, B] = {
    validateParams(maxSampleSize)
    new Impl(maxSampleSize, preAllocate)(map)
  }

  private final class Impl[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean)(map: A => B)
      extends Sampler[A, B] {
    private[this] val rand = new Random()
    private[this] var samples =
      if (preAllocate) new Array[B](maxSampleSize)
      else new Array[B](DefaultInitialSize min maxSampleSize)
    private[this] var open: Boolean         = true
    private[this] var count: Long           = 0
    private[this] var W: Double             = 1.0
    private[this] var nextSampleCount: Long = maxSampleSize

    updateNextSampleCount()

    private def checkOpen(): Unit =
      if (!open) throw new IllegalStateException("use of sampler after calling `result()`")

    // we already know the current size is less than `maxSampleSize` before calling this
    private def ensureSize(): Unit =
      if (!preAllocate) {
        val currentSamples = samples
        val currentSize    = currentSamples.length
        if (currentSize < count) {
          val newSize =
            if (currentSize >= HalfMax) maxSampleSize // `currentSize << 1` is negative, so can't do `min`
            else (currentSize << 1) min maxSampleSize
          val newSamples = new Array[B](newSize)
          System.arraycopy(currentSamples, 0, newSamples, 0, currentSize)
          samples = newSamples
        }
      }

    // Computes and stores the next count at which to sample an element and evict
    //   a previous element. The value computed by this method is not used until
    //   the initial sample is full.
    // Implements Algorithm L from https://en.wikipedia.org/wiki/Reservoir_sampling#An_optimal_algorithm
    private def updateNextSampleCount(): Unit = {
      import java.lang.Math._

      val rand = this.rand
      var W    = this.W
      W = W * exp(log(rand.nextDouble()) / maxSampleSize)
      this.W = W
      nextSampleCount = nextSampleCount + floor(log(rand.nextDouble()) / log(1.0 - W)).toLong + 1L
    }

    override def sample(element: A): Unit = {
      checkOpen()
      val count = this.count + 1
      this.count = count
      val maxSampleSize = this.maxSampleSize
      if (count <= maxSampleSize) { // haven't sampled enough elements yet - just append
        ensureSize()
        // safe because `count <= maxSampleSize`, and `maxSampleSize` is an `Int`
        samples(count.toInt - 1) = map(element)
      } else { // have sampled enough elements, so evict probabilistically
        if (count >= nextSampleCount) {
          samples(rand.nextInt(maxSampleSize)) = map(element)
          updateNextSampleCount()
        }
      }
    }

    override def result(): IndexedSeq[B] = {
      checkOpen()
      open = false
      val count = this.count
      val arr =
        if (count >= maxSampleSize) samples
        else {
          // safe because `count < maxSampleSize`, and `maxSampleSize` is an `Int`
          val size = count.toInt
          val res  = new Array[B](size)
          System.arraycopy(samples, 0, res, 0, size)
          res
        }
      samples = null // allow GC if reference is retained
      ArraySeq.unsafeWrapArray(arr)
    }

    override def isOpen: Boolean = open
  }
}
