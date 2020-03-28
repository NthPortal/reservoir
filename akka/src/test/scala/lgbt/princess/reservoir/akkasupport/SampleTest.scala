package lgbt.princess.reservoir.akkasupport

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.reflect.ClassTag

class SampleTest extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  private var system: ActorSystem       = _
  implicit def actorSystem: ActorSystem = system

  override def beforeAll(): Unit = {
    system = ActorSystem(s"test-${UUID.randomUUID()}")
  }

  override def afterAll(): Unit = {
    system.terminate()
    system = null
  }

  private def sample[A: ClassTag](elements: Seq[A], count: Int): Future[IndexedSeq[A]] =
    Source(elements)
      .viaMat(Sample(maxSampleSize = count, preAllocate = true)(identity))(Keep.right)
      .to(Sink.ignore)
      .run()

  private def sampleRepeatedly[A: ClassTag](elements: Seq[A], sampleSize: Int)(
      times: Int,
  ): Future[Iterator[IndexedSeq[A]]] =
    Future.traverse(Iterator.continually(sample(elements, sampleSize)).take(times))(identity)

  behavior of "a sampling operator"

  it should "not allow negative sample sizes" in {
    an[IllegalArgumentException] should be thrownBy {
      Sample[Int, Int](maxSampleSize = -1, preAllocate = false)(identity)
    }
  }

  it should "not allow sample sizes exceeding VM limits" in {
    an[IllegalArgumentException] should be thrownBy {
      Sample[Int, Int](maxSampleSize = Int.MaxValue, preAllocate = false)(identity)
    }
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
}
