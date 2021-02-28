package lgbt.princess.reservoir
package akkasupport

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream._

import scala.concurrent.{Future, Promise}

/** Implementation for [[Sample]]. */
private final class SampleImpl[A, B](newSampler: => Sampler[A, B])
    extends GraphStageWithMaterializedValue[FlowShape[A, A], Future[IndexedSeq[B]]] {
  import SampleImpl._

  val in: Inlet[A]                    = Inlet[A]("Sample.in")
  val out: Outlet[A]                  = Outlet[A]("Sample.out")
  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override protected def initialAttributes: Attributes = defaultAttr

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
  ): (GraphStageLogic, Future[IndexedSeq[B]]) = {
    val p = Promise[IndexedSeq[B]]()
    val logic: GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
      private val sampler = newSampler

      def onPush(): Unit = {
        val elem = grab(in)
        sampler.sample(elem)
        push(out, elem)
      }

      def onPull(): Unit = pull(in)

      private def tryCompleteSampler(): Unit =
        if (sampler.isOpen) p.trySuccess(sampler.result())

      override def onUpstreamFinish(): Unit = {
        tryCompleteSampler()
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        p.tryFailure(ex)
        failStage(ex)
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        cause match {
          case _: SubscriptionWithCancelException.NonFailureCancellation => tryCompleteSampler()
          case _                                                         => p.tryFailure(cause)
        }
        cancelStage(cause)
      }

      override def postStop(): Unit =
        if (!p.isCompleted) p.failure(new AbruptStageTerminationException(this))

      setHandlers(in, out, this)
    }

    (logic, p.future)
  }

  override def toString: String = "Sample"
}

private object SampleImpl {
  private val defaultAttr = Attributes.name("Sample")
}
