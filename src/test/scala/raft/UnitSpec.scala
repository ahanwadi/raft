package raft

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpecLike
import raft.Raft._

abstract class UnitSpec
    extends ScalaTestWithActorTestKit(ManualTime.config.withFallback(ConfigFactory.load()))
    with AnyWordSpecLike
    with BeforeAndAfter {

  // Always say to election
  def voteYes(member: ServerId, monitorProbe: TestProbe[RaftCmd]): ActorRef[RaftCmd] = {
    spawn(
      behavior = Behaviors.monitor(
        monitor = monitorProbe.ref,
        behavior = Behaviors.receive[Raft.RaftCmd] { (_, cmd) =>
          cmd match {
            case RequestVote(term, candidate, _, replyTo) =>
              replyTo ! RaftReply(term, member, Some(candidate), result = true)
              Behaviors.same
            case req: RaftCmdWithTermExpectingReply =>
              req.replyTo ! RaftReply(0, member, None, result = true)
              Behaviors.same
            case _ =>
              Behaviors.same
          }
        }
      )
    )

  }
}
