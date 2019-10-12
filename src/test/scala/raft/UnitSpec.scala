package raft

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, WordSpecLike}
import raft.Raft.LogIndex

import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.ExpectingReply
import com.typesafe.config.Config

import raft.Raft._

abstract class UnitSpec
    extends ScalaTestWithActorTestKit()
    with WordSpecLike
    with BeforeAndAfter {

  // Always say to election
  def voteYes(member: ServerId, monitorProbe: TestProbe[RaftCmd]) = {
    spawn(
      behavior = Behaviors.monitor(
        monitor = monitorProbe.ref,
        behavior = Behaviors.receive[Raft.RaftCmd] { (_, cmd) =>
          cmd match {
            case RequestVote(term, candidate, _, replyTo) =>
              replyTo ! RaftReply(term, member, Some(candidate), true)
              Behaviors.same
            case req: RaftCmdWithTermExpectingReply =>
              req.replyTo ! RaftReply(0, member, None, true)
              Behaviors.same
            case _ =>
              Behaviors.same
          }
        }
      )
    )

  }
}
