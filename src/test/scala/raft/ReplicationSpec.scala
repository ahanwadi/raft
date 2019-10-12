package raft

import akka.actor.testkit.typed.scaladsl.{
  FishingOutcomes,
  ManualTime,
  TestProbe
}

import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.ExpectingReply
import com.typesafe.config.Config
import org.scalatest.time.SpanSugar._
import raft.Raft._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

class ReplicatorSpec extends UnitSpec() {

  "The replicator" must {

    "replicate no-op cmd" in {

      val monitorProbe = testKit.createTestProbe[RaftCmd]()

      implicit val clusterConfig: Cluster = new Cluster {

        override def otherMembers = Set(Raft.ServerId(80))
        override def members: Set[ServerId] = otherMembers + myId

        override def memberRefs =
          otherMembers.map { member =>
            (
              member,
              voteYes(member, monitorProbe)
            )
          }.toMap

        override val myId: ServerId =
          Raft.ServerId((new Random()).nextInt() & Integer.MAX_VALUE)
      }

      val noOpCmd = Log(LogIndex(1, Index(1)), NoOpCmd())
      val r = spawn(
        Replicator(1, monitorProbe.ref, clusterConfig, Logs(Array(noOpCmd))),
        "SuccessfulElection"
      )

      val t =
        monitorProbe.expectMessageType[Raft.AppendEntries]

      t.leader shouldBe clusterConfig.myId
      t.leaderCommit shouldBe Index(0)
      t.prevLog.index shouldBe Index(0)
      t.log shouldBe Array(noOpCmd)

      val t1 = monitorProbe.expectMessageType[Raft.Committed]
      t1.index shouldBe Index(1)
    }
  }

  "replicate logs from previous terms" is (pending)

  "replicate logs to all followers" is (pending)
}
