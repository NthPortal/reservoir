package lgbt.princess.reservoir

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

class SamplerTest extends AnyFlatSpec with Matchers {
  import SamplerTest._

  private def sample[A: ClassTag](elements: Seq[A], sampleSize: Int)(implicit
      newSampler: NewSampler[A],
  ): IndexedSeq[A] = {
    val sampler = newSampler(sampleSize)
    elements foreach sampler.sample
    sampler.result()
  }

  private def sampleRepeatedly[A: ClassTag: NewSampler](elements: Seq[A], sampleSize: Int)(
      times: Int,
  ): Iterator[IndexedSeq[A]] =
    Iterator.continually(sample(elements, sampleSize)).take(times)

  private def fairReservoirSampler(newSampler: (Int, Int => Int) => Sampler[Int, Int]): Unit = {
    def mkSampler(maxSampleSize: Int): Sampler[Int, Int] = newSampler(maxSampleSize, identity)
    implicit val ns: NewSampler[Int]                     = mkSampler

    it should "not allow negative sample sizes" in {
      an[IllegalArgumentException] should be thrownBy mkSampler(maxSampleSize = -1)
    }

    it should "not allow sample sizes exceeding VM limits" in {
      an[IllegalArgumentException] should be thrownBy mkSampler(maxSampleSize = Int.MaxValue)
    }

    it should "sample all elements if the sample size and number of elements are the same" in {
      sample(1 to 5, 5) should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    }

    it should "sample all elements if there are fewer elements than the sample size" in {
      sample(1 to 5, 6) should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    }

    it should "sample nothing if there are no elements" in {
      sample(Nil, 1) shouldBe empty
    }

    // chance of failure: (1/6)^100 ~= 1.5e-78
    it should "sometimes sample the element after maxSampleSize" in {
      val samples = sampleRepeatedly(1 to 6, 5)(100)
      assert(samples.exists(_ contains 6))
    }

    // chance of failure: (5/6)^400 ~= 2.1e-32
    it should "not always sample the element after maxSampleSize" in {
      val samples = sampleRepeatedly(1 to 6, 5)(400)
      assert(samples.exists(!_.contains(6)))
    }

    // chance of failure: (2/6)^100 ~= 1.9e-48
    it should "sometimes sample elements further past maxSampleSize" in {
      val samples = sampleRepeatedly(1 to 6, 4)(100)
      assert(samples.exists(_ contains 6))
    }

    // chance of failure: (4/6)^200 ~= 6.0e-36
    it should "not always sample elements further past maxSampleSize" in {
      val samples = sampleRepeatedly(1 to 6, 4)(200)
      assert(samples.exists(!_.contains(6)))
    }

    // A given element has a `1/2` chance of being sampled each time; thus, the
    //   probability of a given element being sampled at least `k` times after `n`
    //   (let `n = 1_000_000` here) samples is modeled by the binomial distribution,
    //   but can be approximated using a normal distribution with mean `n/2` and
    //   variance `n/4`.
    // For `n = 1_000_000`, the standard deviation is `500`.
    // Since we want this test to almost never fail, we will require that a difference
    //   between the number of times an element is sampled and the mean (`n/2`) should
    //   not exceed five standard deviations (wikipedia has a handy chart:
    //   https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule#Table_of_numerical_values);
    //   with this threshold, it should fail approximately once in every
    //   1.7 million executions.
    def testSampleHalfEvenly(elements: IndexedSeq[Int]): Unit = {
      require(elements.length % 2 == 0, "must have an even number of elements")

      val sampleTimes           = 1_000_000
      val expectedElementTimes  = sampleTimes / 2
      val stdDev                = math.sqrt(sampleTimes / 4.0)
      val significantDifference = math.ceil(stdDev * 5).toInt

      // assert the number of times each element is sampled is within reasonable range
      sampleRepeatedly(elements, elements.length / 2)(sampleTimes).flatten.toSeq
        .groupMapReduce(identity)(_ => 1)(_ + _)
        .foreachEntry { (_, count) => math.abs(count - expectedElementTimes) should be < significantDifference }
    }

    it should "sample sequential elements relatively evenly" in {
      testSampleHalfEvenly(1 to 10)
    }

    it should "sample random elements relatively evenly" in {
      testSampleHalfEvenly(ArraySeq.fill(10)(Random.nextInt()))
    }

    // A given element has a `1/2` chance of being sampled each time; thus, the
    //   probability of two given elements having the same result (i.e. either
    //   both being sampled or both not being sampled) is
    //   `p = ((c / 2) - 1) / (c - 1)`, where `c` is the number (or "count") of
    //   elements being sampled from. If this sampling is performed `n` times
    //   (let `n = 1_000_000` here), the number of times any two given elements
    //   have the same result is modeled by the binomial distribution. Assuming
    //   `c` is not to be too small (`6` or larger should do fine, as this will
    //   have `p >= 2/5`), the number of times any two given elements have the
    //   same results can be approximated by the normal distribution with mean
    //   `n*p` and variance `n*p*(1-p)`.
    // For `n = 1_000_000` and `c = 10`, the mean is `~=444_444`, and the standard
    //   deviation is `~=497`. As `c` approaches infinity, `p` approaches `1/2`,
    //   the mean approaches `500_000`, and the standard deviation approaches `500`.
    // Since we want this test to almost never fail, we will require that a difference
    //   between the number of times two given elements have the same result and
    //   the mean should not exceed five standard deviations (wikipedia has a handy chart:
    //   https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule#Table_of_numerical_values);
    //   with this threshold, it should fail approximately once in every
    //   1.7 million executions.
    def testSampleHalfIndependently(elements: IndexedSeq[Int]): Unit = {
      require(elements.length % 2 == 0, "must have an even number of elements")

      val sampleTimes = 1_000_000
      val pSameResult = {
        val c = elements.length
        ((c / 2.0) - 1) / (c - 1)
      }
      val expectedSameTimes     = math.round(sampleTimes * pSameResult).toInt
      val stdDev                = math.sqrt(sampleTimes * pSameResult * (1 - pSameResult))
      val significantDifference = math.ceil(stdDev * 5).toInt

      val sameSampleResults: mutable.Map[(Int, Int), Int] = mutable.Map.empty.withDefaultValue(0)

      // count the number of times each pair of elements has the same result
      sampleRepeatedly(elements, elements.length / 2)(sampleTimes)
        .map(_.toSet)
        .foreach { sampled =>
          for {
            i <- elements
            j <- elements
            if i != j
            if sampled.contains(i) == sampled.contains(j)
          } {
            val key = (i, j)
            sameSampleResults(key) = sameSampleResults(key) + 1
          }
        }

      // assert the number of times each pair of elements has the same result
      //   is within reasonable range
      sameSampleResults.foreachEntry { (_, count) =>
        math.abs(count - expectedSameTimes) should be < significantDifference
      }
    }

    it should "sample sequential elements independently of each other" in {
      testSampleHalfIndependently(1 to 10)
    }

    it should "sample random elements independently of each other" in {
      testSampleHalfIndependently(ArraySeq.fill(10)(Random.nextInt()))
    }
  }

  private def singleUseSampler(newSampler: (Int, Int => Int) => Sampler[Int, Int]): Unit = {
    def mkSampler(): Sampler[Int, Int] = newSampler(10, identity)

    it should "throw if `sample(...)` is called after `result()`" in {
      val sampler = mkSampler()
      sampler.result()
      an[IllegalStateException] should be thrownBy sampler.sample(1)
    }

    it should "throw if `result()` is called after `result()`" in {
      val sampler = mkSampler()
      sampler.result()
      an[IllegalStateException] should be thrownBy sampler.result()
    }

    it should "be open before `result()`" in {
      val sampler = mkSampler()
      sampler.isOpen shouldBe true
    }

    it should "not be open after `result()`" in {
      val sampler = mkSampler()
      sampler.result()
      sampler.isOpen shouldBe false
    }
  }

  private def reusableSampler(newSampler: (Int, Int => Int) => Sampler[Int, Int]): Unit = {
    def mkSampler(): Sampler[Int, Int] = newSampler(64, identity)

    it should "allow calling `sample(...)` after `result()`" in {
      val sampler = mkSampler()
      sampler.result()
      noException should be thrownBy sampler.sample(1)
    }

    it should "throw if `result()` is called after `result()`" in {
      val sampler = mkSampler()
      sampler.result()
      noException should be thrownBy sampler.result()
    }

    it should "always be open" in {
      val sampler = mkSampler()
      sampler.isOpen shouldBe true
      sampler.result()
      sampler.isOpen shouldBe true
    }

    it should "not clobber previous results" in {
      val sampler = mkSampler()
      def results(): (IndexedSeq[Int], IndexedSeq[Int], List[Int]) = {
        val res  = sampler.result()
        val list = res.toList
        (res, sampler.result(), list)
      }

      val (res1a, res1b, v1) = results()
      (1 to 32) foreach sampler.sample
      val (res2a, res2b, v2) = results()
      (33 to 64) foreach sampler.sample
      val (res3a, res3b, v3) = results()
      (65 to 128) foreach sampler.sample
      val (res4a, res4b, v4) = results()

      res1a shouldEqual res1b
      res1a shouldEqual v1
      res2a shouldEqual res2b
      res2a shouldEqual v2
      res3a shouldEqual res3b
      res3a shouldEqual v3
      res4a shouldEqual res4b
      res4a shouldEqual v4
    }
  }

  private def elementSampler(newSampler: (Int, Int => Int) => Sampler[Int, Int]): Unit = {
    it should "allow duplicate elements" in {
      val sampler = newSampler(10, identity)
      Iterator
        .continually(1)
        .take(10)
        .foreach(sampler.sample)
      sampler.result() shouldEqual Seq.fill(10)(1)
    }
  }

  private def distinctValueSampler(newSampler: (Int, Int => Int) => Sampler[Int, Int]): Unit = {
    it should "not allow duplicate elements" in {
      val sampler = newSampler(10, identity)
      Iterator
        .continually(1)
        .take(10)
        .foreach(sampler.sample)
      sampler.result() shouldEqual Seq(1)
    }
  }

  behavior of "a single-use reservoir sampler allowing duplicates"
  it should behave like fairReservoirSampler(Sampler[Int, Int](_)(_))
  it should behave like singleUseSampler(Sampler[Int, Int](_)(_))
  it should behave like elementSampler(Sampler[Int, Int](_)(_))

  behavior of "a single-use reservoir sampler allowing duplicates (pre-allocating)"
  it should behave like fairReservoirSampler(Sampler[Int, Int](_)(_))
  it should behave like singleUseSampler(Sampler[Int, Int](_)(_))
  it should behave like elementSampler(Sampler[Int, Int](_)(_))

  behavior of "a reusable reservoir sampler allowing duplicates"
  it should behave like fairReservoirSampler(Sampler[Int, Int](_, reusable = true)(_))
  it should behave like reusableSampler(Sampler[Int, Int](_, reusable = true)(_))
  it should behave like elementSampler(Sampler[Int, Int](_, reusable = true)(_))

  behavior of "a reusable reservoir sampler allowing duplicates (pre-allocating)"
  it should behave like fairReservoirSampler(Sampler[Int, Int](_, preAllocate = true, reusable = true)(_))
  it should behave like reusableSampler(Sampler[Int, Int](_, preAllocate = true, reusable = true)(_))
  it should behave like elementSampler(Sampler[Int, Int](_, preAllocate = true, reusable = true)(_))

  behavior of "a single-use reservoir sampler of distinct values"
  it should behave like fairReservoirSampler(Sampler.distinct[Int, Int](_)(_))
  it should behave like singleUseSampler(Sampler.distinct[Int, Int](_)(_))
  it should behave like distinctValueSampler(Sampler.distinct[Int, Int](_)(_))

  behavior of "a reusable reservoir sampler of distinct values"
  it should behave like fairReservoirSampler(Sampler.distinct[Int, Int](_, reusable = true)(_))
  it should behave like reusableSampler(Sampler.distinct[Int, Int](_, reusable = true)(_))
  it should behave like distinctValueSampler(Sampler.distinct[Int, Int](_, reusable = true)(_))
}

private object SamplerTest {
  trait NewSampler[A] {
    def apply(maxSampleSize: Int): Sampler[A, A]
  }
}
