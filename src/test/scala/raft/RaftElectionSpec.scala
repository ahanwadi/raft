package raft

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit}
import com.typesafe.config.Config
import org.junit.runner.RunWith
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class RaftElectionSpec extends ScalaTestWithActorTestKit() with WordSpecLike {

  "Raft Server" must {
    lazy val probe = createTestProbe[Raft.ClientProto]()
    val manualTime: ManualTime = ManualTime()

    val raftConfig: Config = system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

    "start in a follower mode" in {
      val r = spawn(raft.Raft(), "test")
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(0, 0, "Follower"))
    }

    "transition to candidate after heartbeat timeout" in {
      val r = spawn(raft.Raft())
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(0, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = 0, term = 1, mode = "Candidate"))

    }

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

    "should vote if not voted when in follower mode" in {
      val myId = (new Random()).nextInt()
      val r = spawn(raft.Raft(Some(myId)))
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      r ! Raft.RequestVote(0, 0, probe.ref)
      probe.expectMessage(Raft.RaftReply(0, myId, Some(0), true))

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 0, mode = "Follower"))
    }

    "reject RequestVote as a candidate in the same term" in {
      val myId = (new Random()).nextInt()
      val r = spawn(raft.Raft(Some(myId)))
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 1, mode = "Candidate"))

      r ! Raft.RequestVote(term = 1, candidate = 0, replyTo = probe.ref)
      probe.expectMessage(Raft.RaftReply(term = 1, voter = myId, votedFor = Some(myId), result = false))

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 1, mode = "Candidate"))

    }

    "should not vote if already voted in the current term" in {
      val myId = (new Random()).nextInt()
      val r = spawn(raft.Raft(Some(myId)))
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      r ! Raft.RequestVote(term = 0, candidate = 0, replyTo = probe.ref)
      probe.expectMessage(Raft.RaftReply(term = 0, voter = myId, votedFor = Some(0), result = true))

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 0, mode = "Follower"))

      r ! Raft.RequestVote(term = 0, candidate = 1, replyTo = probe.ref)
      probe.expectMessage(Raft.RaftReply(term = 0, voter = myId, votedFor = Some(0), result = false))
    }

    "should accept newer leader" in {
      val myId = (new Random()).nextInt()
      val r = spawn(raft.Raft(Some(myId)))
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      r ! Raft.AppendEntries(term = 1, leader = 1, replyTo = probe.ref)
      probe.expectMessage(Raft.RaftReply(term = 1, voter = myId, votedFor = None, result = true))

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 1, mode = "Follower"))
    }


    "should reject requests with stale terms" in {
      val myId = (new Random()).nextInt()
      val r = spawn(raft.Raft(Some(myId)))
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(id = myId, term = 1, mode = "Candidate"))

      r ! Raft.AppendEntries(term = 0, leader = 1, replyTo = probe.ref)
      probe.expectMessage(Raft.RaftReply(term = 1, voter = myId, votedFor = None, result = false))

    }

  }
}

