package raft

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit}
import com.typesafe.config.Config
import org.junit.runner.RunWith
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.{Duration, FiniteDuration}

@RunWith(classOf[JUnitRunner])
class RaftElection extends ScalaTestWithActorTestKit() with WordSpecLike {

  lazy val probe = createTestProbe[Raft.ClientProto]()
  val manualTime: ManualTime = ManualTime()

  val raftConfig: Config = system.settings.config.getConfig("raft")
  val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

  "Raft Server" must {
    "start in a follower mode" in {
      val r = spawn(raft.Raft(), "test")
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(0, 0, "Follower"))
    }
  }

  "Raft Server" must {
    "transition to candidate after heartbeat timeout" in {
      val r = spawn(raft.Raft())
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(0, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = 0, term = 1, mode = "Candidate"))

    }
  }

  "Raft Server" must {
    "transition to candidate with next term after no votes are received within heartbeat timeout" in {
      val myId = 10
      val r = spawn(raft.Raft(Some(myId)))
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 1, mode = "Candidate"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 2, mode = "Candidate"))

    }
  }
}

