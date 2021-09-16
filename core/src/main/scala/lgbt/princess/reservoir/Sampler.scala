package lgbt.princess.reservoir

import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.byteswap64

/**
 * Utility for randomly sampling from a stream of elements, without keeping any elements other than those sampled in
 * memory; this is known as [[https://en.wikipedia.org/wiki/Reservoir_sampling reservoir sampling]].
 *
 * @note
 *   Instances of this type are NOT reusable unless otherwise specified; that is, methods other than `isOpen` MUST NOT
 *   be invoked after calling `result()` once unless otherwise specified. Reusable instances may leak memory if you
 *   retain references to them after you are finished using them.
 * @note
 *   Instances of this type are NOT thread-safe unless otherwise specified.
 *
 * @tparam A
 *   the type of elements being sampled from
 * @tparam B
 *   the type of sample elements being stored
 */
trait Sampler[A, B] {

  /**
   * Sample from another element, probabilistically.
   *
   * For a sample of size `k`, a total of `n` elements already sampled, if this sampler allows duplicates, this
   * particular element has a `k/n` chance of being in the final sample.
   *
   * For a sample size of `k`, a total of `n` distinct elements already sampled, if this sampler does not allow
   * duplicates, this distinct value has a `k/n` chance of being in the final sample.
   */
  @throws[IllegalStateException]
  def sample(element: A): Unit

  /**
   * Sample each of the elements in a collection, probabilistically.
   *
   * For a sample of size `k`, a total of `n` elements already sampled, if this sampler allows duplicates, the next
   * element sampled has a `k/n` chance of being in the final sample.
   *
   * For a sample size of `k`, a total of `n` distinct elements already sampled, if this sampler does not allow
   * duplicates, the next distinct value has a `k/n` chance of being in the final sample.
   */
  @throws[IllegalStateException]
  def sampleAll(elements: IterableOnce[A]): Unit = elements.iterator foreach sample

  /**
   * @note
   *   methods other than [[isOpen]] (including this one) MUST NOT be called after calling this method once unless
   *   otherwise specified.
   * @return
   *   the sampled elements
   */
  @throws[IllegalStateException]
  def result(): IndexedSeq[B]

  /**
   * Whether or not this sampler can still sample elements and return a resulting sample.
   *
   * Methods other than this one MUST NOT be called after this method returns `false`.
   */
  def isOpen: Boolean
}

object Sampler {
  private final val MaxSize            = Int.MaxValue - 2 // hotspot VM limit for `Array` size
  private final val DefaultInitialSize = 16
  private final val HalfMax = 1 << 30 // doubling this gives a negative, which is a pain to work with

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
   * Creates a [[Sampler reservoir sampler]] that samples elements with equal probability, and allows duplicate elements
   * in the sample.
   *
   * @note
   *   Instances returned by this method are NOT reusable; methods other than `isOpen` MUST NOT be invoked after calling
   *   `result()` once.
   * @note
   *   Instances returned by this method are NOT thread-safe.
   *
   * @param maxSampleSize
   *   the maximum number of elements to keep in the sample; if at least this many elements are sampled, this will be
   *   the size of the final sample
   * @param preAllocate
   *   whether or not to pre-allocate space for the maximum number of sampled elements
   * @param reusable
   *   whether or not the returned instance should support further calls to [[Sampler.sample `sample(...)`]] and
   *   [[Sampler.result `result()`]] after calling `result()` once
   * @param map
   *   a mapping function to apply to elements being sampled; this may be called more than `maxSampleSize` times
   * @tparam A
   *   the type of elements being sampled from
   * @tparam B
   *   the type of sample elements being stored
   * @throws scala.IllegalArgumentException
   *   if `maxSampleSize` is negative or exceeds VM limit
   * @throws scala.NullPointerException
   *   if `map` is `null`
   * @return
   *   an [[Sampler.isOpen open]] reservoir sampler
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def apply[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean = false, reusable: Boolean = false)(
      map: A => B,
  ): Sampler[A, B] = {
    validateNonDistinctParams(maxSampleSize, map)
    if (reusable) new MultiResultRandomElements[A, B](maxSampleSize, preAllocate)(map)
    else new SingleUseRandomElements[A, B](maxSampleSize, preAllocate)(map)
  }

  /**
   * Creates a [[Sampler reservoir sampler]] that samples distinct values with equal probability; it does not allow
   * duplicate elements in the sample.
   *
   * @note
   *   Instances returned by this method are NOT thread-safe.
   * @note
   *   Instances returned by this method are less efficient both in memory and CPU than those returned by [[apply]], due
   *   to the need to sample distinct elements.
   *
   * @param maxSampleSize
   *   the maximum number of elements to keep in the sample; if at least this many elements are sampled, this will be
   *   the size of the final sample
   * @param reusable
   *   whether or not the returned instance should support further calls to [[Sampler.sample `sample(...)`]] and
   *   [[Sampler.result `result()`]] after calling `result()` once
   * @param map
   *   a mapping function to apply to elements being sampled; it is called for each element sampled
   * @param hash
   *   a function used to hash elements of the sample. By default, `B#hashCode()` is used, but if `B#hashCode()` does
   *   not reliably generate different values for different elements, a custom hash function should be provided.
   *   Additionally, if a cheaper or higher-granularity hash function exists, that should be used instead.
   * @tparam A
   *   the type of elements being sampled from
   * @tparam B
   *   the type of sample elements being stored
   * @throws scala.IllegalArgumentException
   *   if `maxSampleSize` is negative or exceeds VM limit
   * @throws scala.NullPointerException
   *   if `map` or `hash` is `null`
   * @return
   *   an [[Sampler.isOpen open]] reservoir sampler
   */
  @throws[IllegalArgumentException]
  @throws[NullPointerException]
  def distinct[A, B: ClassTag](maxSampleSize: Int, reusable: Boolean = false)(
      map: A => B,
      hash: B => Long = defaultHashFunction,
  ): Sampler[A, B] = {
    validateDistinctParams(maxSampleSize, map, hash)
    if (reusable) new MultiResultRandomValues[A, B](maxSampleSize)(map, hash)
    else new SingleUseRandomValues[A, B](maxSampleSize)(map, hash)
  }

  private sealed trait SingleUse { self: Sampler[_, _] =>
    private[this] var open: Boolean = true

    protected final def checkOpen(): Unit =
      if (!open) throw new IllegalStateException("use of sampler after calling `result()`")

    protected final def close(): Unit = {
      checkOpen()
      open = false
    }

    override final def isOpen: Boolean = open
  }

  private sealed abstract class RandomElements[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean)(
      map: A => B,
  ) extends Sampler[A, B] {
    private[this] val rand = new Random()
    protected[this] var samples: Array[B] =
      if (preAllocate) new Array[B](maxSampleSize)
      else new Array[B](DefaultInitialSize min maxSampleSize)
    private[this] var count: Long           = 0
    private[this] var W: Double             = 1.0
    private[this] var nextSampleCount: Long = maxSampleSize

    updateNextSampleCount()

    // we already know the current size is less than `maxSampleSize` before calling this
    private[this] def ensureSize(count: Int): Unit =
      if (!preAllocate) {
        val currentSamples = samples
        val currentSize    = currentSamples.length
        if (currentSize <= count) {
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
    private[this] def updateNextSampleCount(): Unit = {
      import java.lang.Math._

      val rand = this.rand
      var W    = this.W
      W = W * exp(log(rand.nextDouble()) / maxSampleSize)
      this.W = W
      nextSampleCount = nextSampleCount + floor(log(rand.nextDouble()) / log(1.0 - W)).toLong + 1L
    }

    private[this] def sampleAndAppend(idx: Int)(element: A): Unit = {
      ensureSize(idx)
      samples(idx) = map(element)
    }

    private[this] def sampleWithEviction(element: A): Unit = {
      samples(rand.nextInt(maxSampleSize)) = map(element)
      updateNextSampleCount()
    }

    protected[this] final def sampleImpl(element: A): Unit = {
      val prevCount = this.count
      val newCount  = prevCount + 1
      this.count = newCount
      val maxSampleSize = this.maxSampleSize
      if (newCount <= maxSampleSize) { // haven't sampled enough elements yet - just append
        // safe because `prevCount < maxSampleSize`, and `maxSampleSize` is an `Int`
        sampleAndAppend(prevCount.toInt)(element)
      } else { // have sampled enough elements, so evict probabilistically
        if (newCount >= nextSampleCount) sampleWithEviction(element)
      }
    }

    @tailrec private[this] def sampleIndexed(seq: collection.IndexedSeq[A], start: Int, len: Int, cnt: Long): Unit = {
      if (len > 0) {
        val nextSampleCount = this.nextSampleCount
        val nextIdxOffset   = nextSampleCount - cnt
        if (len >= nextIdxOffset) {
          // safe because `nextIdxOffset <= len`, and `len` is an `Int`
          val offsetInt = nextIdxOffset.toInt
          val nextStart = start + offsetInt
          sampleWithEviction(seq(nextStart - 1))
          sampleIndexed(seq, nextStart, len - offsetInt, nextSampleCount)
        }
      }
    }

    @tailrec private[this] def sampleIterator(dropFromIt: Iterator[A], len: Int, cnt: Long): Unit = {
      if (len > 0) {
        val nextSampleCount = this.nextSampleCount
        val nextIdxOffset   = nextSampleCount - cnt
        if (len >= nextIdxOffset) {
          // safe because `nextIdxOffset <= len`, and `len` is an `Int`
          val offsetInt = nextIdxOffset.toInt
          val it        = dropFromIt.drop(offsetInt - 1)
          sampleWithEviction(it.next())
          sampleIterator(it, len - offsetInt, nextSampleCount)
        }
      }
    }

    protected[this] final def sampleAllImpl(elements: IterableOnce[A]): Unit = {
      val ks = elements.knownSize
      if (ks > 0) {
        val startCount      = this.count
        val maxSampleSize   = this.maxSampleSize
        var i               = 0
        var it: Iterator[A] = null
        if (startCount < maxSampleSize) { // haven't sampled enough elements yet - just append
          // safe because `startCount < maxSampleSize`, and `maxSampleSize` is an `Int`
          var count = startCount.toInt
          it = elements.iterator
          while (count < maxSampleSize && it.hasNext) {
            sampleAndAppend(count)(it.next())
            count += 1
            i += 1
          }
        }
        elements match {
          case seq: collection.IndexedSeq[A] => sampleIndexed(seq, start = i, len = ks - i, cnt = startCount + i)
          case _ =>
            if (it == null) it = elements.iterator
            sampleIterator(it, len = ks - i, cnt = startCount + i)
        }
        this.count = startCount + ks
      } else if (ks < 0) {
        elements.iterator foreach sampleImpl
      }
    }

    protected[this] final def resultImpl: ArraySeq[B] = {
      val count    = this.count
      val _samples = samples
      val arr =
        if (count >= _samples.length) _samples
        else {
          // safe because `count < samples.size`, and Array sizes are `Int`s
          val size = count.toInt
          val res  = new Array[B](size)
          System.arraycopy(_samples, 0, res, 0, size)
          res
        }
      ArraySeq.unsafeWrapArray(arr)
    }
  }

  private final class SingleUseRandomElements[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean)(map: A => B)
      extends RandomElements[A, B](maxSampleSize, preAllocate)(map)
      with SingleUse {
    def sample(element: A): Unit = {
      checkOpen()
      sampleImpl(element)
    }
    override def sampleAll(elements: IterableOnce[A]): Unit = {
      checkOpen()
      sampleAllImpl(elements)
    }
    def result(): IndexedSeq[B] = {
      close()
      val res = resultImpl
      samples = null // allow GC if reference is retained
      res
    }
  }

  private final class MultiResultRandomElements[A, B: ClassTag](maxSampleSize: Int, preAllocate: Boolean)(map: A => B)
      extends RandomElements[A, B](maxSampleSize, preAllocate)(map) {
    private[this] var aliased: Boolean = false

    private[this] def ensureUnaliased(): Unit =
      if (aliased) {
        val _samples = samples
        val len      = _samples.length
        val copy     = new Array[B](len)
        System.arraycopy(_samples, 0, copy, 0, len)
        samples = copy
        aliased = false
      }

    def sample(element: A): Unit = {
      ensureUnaliased()
      sampleImpl(element)
    }
    override def sampleAll(elements: IterableOnce[A]): Unit = {
      ensureUnaliased()
      sampleAllImpl(elements)
    }
    def result(): IndexedSeq[B] = {
      val res = resultImpl
      if (res.unsafeArray eq samples) aliased = true
      res
    }
    def isOpen: Boolean = true
  }

  private sealed abstract class RandomValues[A, B: ClassTag](maxSampleSize: Int)(map: A => B, hash: B => Long)
      extends Sampler[A, B] {
    private[this] val (r0, r1) = {
      val rand = new Random()
      (rand.nextLong(), rand.nextLong())
    }
    protected[this] var samples: mutable.PriorityQueue[(B, Long)] =
      mutable.PriorityQueue.empty[(B, Long)](Ordering.by(_._2))
    protected[this] var elements: mutable.Set[B] = mutable.Set.empty
    private[this] var maxHash                    = Long.MinValue

    def sample(element: A): Unit = {
      val elem     = map(element)
      val elemHash = byteswap64(r1 ^ byteswap64(r0 ^ hash(elem))) // randomly map hash to another value (hopefully)
      if (samples.size < maxSampleSize) {
        if (!elements.contains(elem)) {
          samples += ((elem, elemHash))
          elements += elem
          maxHash = maxHash max elemHash
        }
      } else if (elemHash < maxHash && !elements.contains(elem)) {
        elements -= samples.dequeue()._1
        samples += ((elem, elemHash))
        elements += elem
        maxHash = samples.head._2
      }
    }

    def result(): IndexedSeq[B] = elements to ArraySeq
  }

  private final class SingleUseRandomValues[A, B: ClassTag](maxSampleSize: Int)(map: A => B, hash: B => Long)
      extends RandomValues[A, B](maxSampleSize)(map, hash)
      with SingleUse {
    override def sample(element: A): Unit = {
      checkOpen()
      super.sample(element)
    }
    override def result(): IndexedSeq[B] = {
      close()
      val res = super.result()
      samples = null // allow GC if reference is retained
      elements = null
      res
    }
  }

  private final class MultiResultRandomValues[A, B: ClassTag](maxSampleSize: Int)(map: A => B, hash: B => Long)
      extends RandomValues[A, B](maxSampleSize)(map, hash) {
    def isOpen: Boolean = true
  }
}
