package lgbt.princess.reservoir.akkasupport

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, Succeeded}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Random

class SampleTest extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  import SampleTest._

  private var system: ActorSystem       = _
  implicit def actorSystem: ActorSystem = system

  override def beforeAll(): Unit = {
    system = ActorSystem(s"test-${UUID.randomUUID()}")
  }

  override def afterAll(): Unit = {
    system.terminate()
    system = null
  }

  private def sampleGraph[A: ClassTag](elements: Seq[A], count: Int)(implicit
      newSampler: NewSampler[A],
  ): RunnableGraph[Future[IndexedSeq[A]]] =
    Source(elements)
      .viaMat(newSampler(maxSampleSize = count))(Keep.right)
      .to(Sink.ignore)

  private def sample[A: ClassTag: NewSampler](elements: Seq[A], count: Int): Future[IndexedSeq[A]] =
    sampleGraph(elements, count).run()

  private def sampleRepeatedly[A: ClassTag: NewSampler](elements: Seq[A], sampleSize: Int)(
      times: Int,
  ): Future[Iterator[IndexedSeq[A]]] = {
    val graph = sampleGraph(elements, sampleSize)
    Future.traverse(Iterator.continually(graph.run()).take(times))(identity)
  }

  private def fairSamplingOperator(newSampler: (Int, Int => Int) => Flow[Int, Int, Future[IndexedSeq[Int]]]): Unit = {
    def mkSampler(maxSampleSize: Int): Flow[Int, Int, Future[IndexedSeq[Int]]] = newSampler(maxSampleSize, identity)
    implicit val ns: NewSampler[Int]                                           = mkSampler(_)

    it should "not allow negative sample sizes" in {
      an[IllegalArgumentException] should be thrownBy mkSampler(maxSampleSize = -1)
    }

    it should "not allow sample sizes exceeding VM limits" in {
      an[IllegalArgumentException] should be thrownBy mkSampler(maxSampleSize = Int.MaxValue)
    }

    it should "sample all elements if the sample size and number of elements are the same" in {
      for (res <- sample(1 to 5, 5))
      yield res should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    }

    it should "sample all elements if there are fewer elements than the sample size" in {
      for (res <- sample(1 to 5, 6))
      yield res should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    }

    it should "sample nothing if there are no elements" in {
      for (res <- sample(Nil, 1)) yield res shouldBe empty
    }

    // chance of failure: (1/6)^100 ~= 1.5e-78
    it should "sometimes sample the element after maxSampleSize" in {
      val future = sampleRepeatedly(1 to 6, 5)(100)
      for (samples <- future) yield assert(samples.exists(_ contains 6))
    }

    // chance of failure: (5/6)^400 ~= 2.1e-32
    it should "not always sample the element after maxSampleSize" in {
      val future = sampleRepeatedly(1 to 6, 5)(400)
      for (samples <- future) yield assert(samples.exists(!_.contains(6)))
    }

    // chance of failure: (2/6)^100 ~= 1.9e-48
    it should "sometimes sample elements further past maxSampleSize" in {
      val future = sampleRepeatedly(1 to 6, 4)(100)
      for (samples <- future) yield assert(samples.exists(_ contains 6))
    }

    // chance of failure: (4/6)^200 ~= 6.0e-36
    it should "not always sample elements further past maxSampleSize" in {
      val future = sampleRepeatedly(1 to 6, 4)(200)
      for (samples <- future) yield assert(samples.exists(!_.contains(6)))
    }

    // A given element has a `1/2` chance of being sampled each time; thus, the
    //   probability of a given element being sampled at least `k` times after `n`
    //   (let `n = 500_000` here) samples is modeled by the binomial distribution,
    //   but can be approximated using a normal distribution with mean `n/2` and
    //   variance `n/4`.
    // For `n = 500_000`, the standard deviation is `~=354`.
    // Since we want this test to almost never fail, we will require that a difference
    //   between the number of times an element is sampled and the mean (`n/2`) should
    //   not exceed five standard deviations (wikipedia has a handy chart:
    //   https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule#Table_of_numerical_values);
    //   with this threshold, it should fail approximately once in every
    //   1.7 million executions.
    def testSampleHalfEvenly(elements: IndexedSeq[Int]): Future[Assertion] = {
      require(elements.length % 2 == 0, "must have an even number of elements")

      val sampleTimes           = 500_000
      val expectedElementTimes  = sampleTimes / 2
      val stdDev                = math.sqrt(sampleTimes / 4.0)
      val significantDifference = math.ceil(stdDev * 5).toInt

      // assert the number of times each element is sampled is within reasonable range
      for (it <- sampleRepeatedly(elements, elements.length / 2)(sampleTimes))
      yield {
        val assertions = it.flatten.toSeq
          .groupMapReduce(identity)(_ => 1)(_ + _)
          .map { case (_, count) =>
            math.abs(count - expectedElementTimes) should be < significantDifference
          }
        assert(assertions.forall(_ == Succeeded))
      }
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
    //   (let `n = 500_000` here), the number of times any two given elements
    //   have the same result is modeled by the binomial distribution. Assuming
    //   `c` is not to be too small (`6` or larger should do fine, as this will
    //   have `p >= 2/5`), the number of times any two given elements have the
    //   same results can be approximated by the normal distribution with mean
    //   `n*p` and variance `n*p*(1-p)`.
    // For `n = 500_000` and `c = 10`, the mean is `~=222_222`, and the standard
    //   deviation is `~=351`. As `c` approaches infinity, `p` approaches `1/2`,
    //   the mean approaches `250_000`, and the standard deviation approaches `~=354`.
    // Since we want this test to almost never fail, we will require that a difference
    //   between the number of times two given elements have the same result and
    //   the mean should not exceed five standard deviations (wikipedia has a handy chart:
    //   https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule#Table_of_numerical_values);
    //   with this threshold, it should fail approximately once in every
    //   1.7 million executions.
    def testSampleHalfIndependently(elements: IndexedSeq[Int]): Future[Assertion] = {
      require(elements.length % 2 == 0, "must have an even number of elements")

      val sampleTimes = 500_000
      val pSameResult = {
        val c = elements.length
        ((c / 2.0) - 1) / (c - 1)
      }
      val expectedSameTimes     = math.round(sampleTimes * pSameResult).toInt
      val stdDev                = math.sqrt(sampleTimes * pSameResult * (1 - pSameResult))
      val significantDifference = math.ceil(stdDev * 5).toInt

      val sameSampleResults: mutable.Map[(Int, Int), Int] = mutable.Map.empty.withDefaultValue(0)

      for (it <- sampleRepeatedly(elements, elements.length / 2)(sampleTimes))
      yield {
        // count the number of times each pair of elements has the same result
        it.map(_.toSet)
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
        val assertions = sameSampleResults.map { case (_, count) =>
          math.abs(count - expectedSameTimes) should be < significantDifference
        }
        assert(assertions.forall(_ == Succeeded))
      }
    }

    it should "sample sequential elements independently of each other" in {
      testSampleHalfIndependently(1 to 10)
    }

    it should "sample random elements independently of each other" in {
      testSampleHalfIndependently(ArraySeq.fill(10)(Random.nextInt()))
    }
  }

  private def elementSampler(newSampler: (Int, Int => Int) => Flow[Int, Int, Future[IndexedSeq[Int]]]): Unit = {
    def mkSampler(): Flow[Int, Int, Future[IndexedSeq[Int]]] = newSampler(10, identity)

    it should "allow duplicate elements" in {
      val future = Source
        .fromIterator(() => Iterator.continually(1))
        .take(10)
        .viaMat(mkSampler())(Keep.right)
        .to(Sink.ignore)
        .run()

      for (sample <- future) yield sample shouldEqual Seq.fill(10)(1)
    }
  }

  behavior of "a sampling operator that allows duplicates"
  it should behave like fairSamplingOperator(Sample[Int, Int](_)(_))
  it should behave like elementSampler(Sample[Int, Int](_)(_))

  behavior of "a sampling operator that allows duplicates (pre-allocating)"
  it should behave like fairSamplingOperator(Sample[Int, Int](_, preAllocate = true)(_))
  it should behave like elementSampler(Sample[Int, Int](_, preAllocate = true)(_))

  behavior of "a sampling operator of distinct values"
  it should behave like fairSamplingOperator(Sample.distinct[Int, Int](_)(_))
  it should "not allow duplicate elements" in {
    val future = Source
      .fromIterator(() => Iterator.continually(1))
      .take(10)
      .viaMat(Sample.distinct[Int, Int](10)(identity))(Keep.right)
      .to(Sink.ignore)
      .run()

    for (sample <- future) yield sample shouldEqual Seq(1)
  }
}

private object SampleTest {
  trait NewSampler[A] {
    def apply(maxSampleSize: Int): Flow[A, A, Future[IndexedSeq[A]]]
  }
}
