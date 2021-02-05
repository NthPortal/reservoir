package lgbt.princess.reservoir

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.byteswap64

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
   * For a sample of size `k`, a total of `n` elements sampled, if this
   * sampler allows duplicates, this particular element has a `k/n` chance
   * of being in the final sample.
   *
   * For a sample size of `k`, a total of `n` distinct elements sampled, if
   * this sampler does not allow duplicates, this distinct value has a `k/n`
   * chance of being in the final sample.
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
   *
   * Methods other than this one MUST NOT be called after this method
   * returns `false`.
   */
  def isOpen: Boolean
}

object Sampler {
  private final val MaxSize            = Int.MaxValue - 2 // hotspot VM limit for `Array` size
  private final val DefaultInitialSize = 16
  private final val HalfMax            = 1 << 30          // doubling this gives a negative, which is a pain to work with

  private[reservoir] def defaultHashFunction[B]: B => Long = _.hashCode().toLong

  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  private def validateSharedParams[A, B](maxSampleSize: Int, map: A => B): Unit = {
    require(maxSampleSize <= MaxSize, "maxSampleSize exceeds VM limit")
    require(maxSampleSize > 0, "maxSampleSize must be positive")
    if (map == null) throw new NullPointerException("`map` cannot be `null`")
  }

  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  private[reservoir] def validateNonDistinctParams[A, B](maxSampleSize: Int, map: A => B): Unit =
    validateSharedParams(maxSampleSize, map)

  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  private[reservoir] def validateDistinctParams[A, B](maxSampleSize: Int, map: A => B, hash: B => Long): Unit = {
    validateSharedParams(maxSampleSize, map)
    if (hash == null) throw new NullPointerException("`hash` cannot be `null`")
  }

  /**
   * Creates a [[Sampler reservoir sampler]] that samples elements with
   * equal probability, and allows duplicate elements in the sample.
   *
   * @note Instances returned by this method are NOT reusable; methods other than
   *       `isOpen` MUST NOT be invoked after calling `result()` once.
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
   * @throws scala.NullPointerException     if `map` is `null`
   * @return an [[Sampler.isOpen open]] reservoir sampler
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def apply[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean = false)(
      map: A => B,
  ): Sampler[A, B] = {
    validateNonDistinctParams(maxSampleSize, map)
    new RandomElements(maxSampleSize, preAllocate)(map)
  }

  /**
   * Creates a [[Sampler reservoir sampler]] that samples distinct values with
   * equal probability; it does not allow duplicate elements in the sample.
   *
   * @note Instances returned by this method are NOT reusable; methods other than
   *       `isOpen` MUST NOT be invoked after calling `result()` once.
   * @note Instances returned by this method are NOT thread-safe.
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
   * @return an [[Sampler.isOpen open]] reservoir sampler
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def distinct[A, B: ClassTag](maxSampleSize: Int)(
      map: A => B,
      hash: B => Long = defaultHashFunction,
  ): Sampler[A, B] = {
    validateDistinctParams(maxSampleSize, map, hash)
    new RandomValues[A, B](maxSampleSize)(map, hash)
  }

  private abstract class Base[A, B] extends Sampler[A, B] {
    private[this] var open: Boolean = true

    protected final def checkOpen(): Unit =
      if (!open) throw new IllegalStateException("use of sampler after calling `result()`")

    protected final def close(): Unit = {
      checkOpen()
      open = false
    }

    override final def isOpen: Boolean = open
  }

  private final class RandomElements[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean)(map: A => B)
      extends Base[A, B] {
    private[this] val rand = new Random()
    private[this] var samples =
      if (preAllocate) new Array[B](maxSampleSize)
      else new Array[B](DefaultInitialSize min maxSampleSize)
    private[this] var count: Long           = 0
    private[this] var W: Double             = 1.0
    private[this] var nextSampleCount: Long = maxSampleSize

    updateNextSampleCount()

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
      close()
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
  }

  private final class RandomValues[A, B: ClassTag](maxSampleSize: Int)(map: A => B, hash: B => Long)
      extends Base[A, B] {
    private[this] val (r0, r1) = {
      val rand = new Random()
      (rand.nextLong(), rand.nextLong())
    }
    private[this] var sample   = mutable.PriorityQueue.empty[(B, Long)](Ordering.by(_._2))
    private[this] var elements = mutable.Set.empty[B]
    private[this] var maxHash  = Long.MinValue

    def sample(element: A): Unit = {
      checkOpen()
      val elem     = map(element)
      val elemHash = byteswap64(r1 ^ byteswap64(r0 ^ hash(elem))) // randomly map hash to another value (hopefully)
      if (sample.size < maxSampleSize) {
        if (!elements.contains(elem)) {
          sample += ((elem, elemHash))
          elements += elem
          maxHash = maxHash max elemHash
        }
      } else if (elemHash < maxHash && !elements.contains(elem)) {
        elements -= sample.dequeue()._1
        sample += ((elem, elemHash))
        elements += elem
        maxHash = sample.head._2
      }
    }

    def result(): IndexedSeq[B] = {
      close()
      val res = elements to ArraySeq
      sample = null // allow GC if reference is retained
      elements = null
      res
    }
  }
}
