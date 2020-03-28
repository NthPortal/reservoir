package lgbt.princess.reservoir

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap
import scala.reflect.ClassTag

class SamplerTest extends AnyFlatSpec with Matchers {
  private def sample[A: ClassTag](elements: Seq[A], sampleSize: Int): IndexedSeq[A] = {
    val sampler = Sampler[A, A](maxSampleSize = sampleSize, preAllocate = true)(identity)
    elements foreach sampler.sample
    sampler.result()
  }

  private def sampleRepeatedly[A: ClassTag](elements: Seq[A], sampleSize: Int)(times: Int): Iterator[IndexedSeq[A]] =
    Iterator.continually(sample(elements, sampleSize)).take(times)

  behavior of "a reservoir sampler that allows duplicates"

  it should "not allow negative sample sizes" in {
    an[IllegalArgumentException] should be thrownBy {
      Sampler[Int, Int](maxSampleSize = -1, preAllocate = false)(identity)
    }
  }

  it should "not allow sample sizes exceeding VM limits" in {
    an[IllegalArgumentException] should be thrownBy {
      Sampler[Int, Int](maxSampleSize = Int.MaxValue, preAllocate = false)(identity)
    }
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

  it should "sample relatively evenly" in {
    sampleRepeatedly(1 to 10, 5)(1000000).flatten.toSeq
      .groupMapReduce(identity)(_ => 1)(_ + _)
      .to(SortedMap)
      .foreach(println)
  }
}
