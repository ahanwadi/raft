package raft

import akka.actor.testkit.typed.scaladsl.{
  FishingOutcomes,
  ManualTime,
  ScalaTestWithActorTestKit,
  TestProbe
}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.ExpectingReply
import com.typesafe.config.Config
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, WordSpecLike}
import raft.Raft._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class RaftElectionSpec
    extends ScalaTestWithActorTestKit()
    with WordSpecLike
    with BeforeAndAfter {

  "Raft Server" must {

    var probe: TestProbe[TestProto] = null
    val manualTime: ManualTime = ManualTime()
    val raftConfig: Config = system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration =
      Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())
    var myId: ServerId = ServerId(0)
    implicit var clusterConfig: Cluster = null

    before {
      probe = createTestProbe[Raft.TestProto]()

      myId = ServerId((new Random()).nextInt() & Integer.MAX_VALUE)

      clusterConfig = Cluster(myId)
    }

    "start in a follower mode" in {
      val r = spawn(raft.Raft(), "test")
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))
    }

    "transition to candidate after heartbeat timeout" in {
      val r = spawn(raft.Raft())
      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
      )

    }

    "transition to candidate with next term after no votes are received within heartbeat timeout" in {
      val r = spawn(raft.Raft())

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
      )

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 2, mode = "Candidate")
      )

    }

    "vote if not voted when in follower mode" in {
      val r = spawn(raft.Raft())
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      r ! Raft.RequestVote(0, Raft.ServerId(0), LogIndex(), probe.ref)
      probe.expectMessage(Raft.RaftReply(0, clusterConfig.myId, Some(Raft.ServerId(0)), true))

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 0, mode = "Follower")
      )
    }

    "reject RequestVote as a candidate in the same term" in {
      val r = spawn(raft.Raft())
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
      )

      r ! Raft.RequestVote(
        term = 1,
        candidate = Raft.ServerId(0),
        LogIndex(),
        replyTo = probe.ref
      )
      probe.expectMessage(
        Raft.RaftReply(
          term = 1,
          voter = clusterConfig.myId,
          votedFor = Some(clusterConfig.myId),
          result = false
        )
      )

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
      )

    }

    " not vote if already voted in the current term" in {
      val r = spawn(raft.Raft())
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      r ! Raft.RequestVote(
        term = 0,
        candidate = Raft.ServerId(0),
        LogIndex(),
        replyTo = probe.ref
      )
      probe.expectMessage(
        Raft.RaftReply(
          term = 0,
          voter = clusterConfig.myId,
          votedFor = Some(Raft.ServerId(0)),
          result = true
        )
      )

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 0, mode = "Follower")
      )

      r ! Raft.RequestVote(
        term = 0,
        candidate = Raft.ServerId(1),
        LogIndex(),
        replyTo = probe.ref
      )
      probe.expectMessage(
        Raft.RaftReply(
          term = 0,
          voter = clusterConfig.myId,
          votedFor = Some(ServerId(0)),
          result = false
        )
      )
    }

    "accept newer leader" in {
      val r = spawn(raft.Raft(), this.suiteName)
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      r ! Raft.AppendEntries(
        term = 1,
        leader = Raft.ServerId(1),
        replyTo = probe.ref,
        prevLog = LogIndex(),
        leaderCommit = LogIndex()
      )
      probe.expectMessage(
        Raft.RaftReply(
          term = 1,
          voter = clusterConfig.myId,
          votedFor = None,
          result = true
        )
      )

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Follower")
      )
    }

    " reject requests with stale terms" in {
      val r = spawn(raft.Raft())
      val probe = createTestProbe[Raft.RaftCmd]()

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(
        Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
      )

      r ! Raft.AppendEntries(
        term = 0,
        leader = Raft.ServerId(1),
        replyTo = probe.ref,
        prevLog = LogIndex(),
        leaderCommit = LogIndex()
      )
      probe.expectMessage(
        Raft.RaftReply(
          term = 1,
          voter = clusterConfig.myId,
          votedFor = None,
          result = false
        )
      )

    }

    " get elected as leader unanimously" in {

      val monitorProbe = testKit.createTestProbe[RaftCmd]()

      implicit val clusterConfig: Cluster = new Cluster {

        def otherMembers = Set(Raft.ServerId(20), Raft.ServerId(30))
        override def members: Set[ServerId] = otherMembers + myId

        override def memberRefs =
          otherMembers.map { member =>
            (
              member,
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
            )
          }.toMap

        override val myId: ServerId = Raft.ServerId((new Random()).nextInt() & Integer.MAX_VALUE)
      }

      val r = spawn(raft.Raft(), "SuccessfulElection")

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)
      eventually(
        timeout(scaled(electionTimeout * 2)),
        interval(scaled(2 seconds))
      ) {
        r ! Raft.GetState(probe.ref)
        val t = probe.expectMessageType[Raft.CurrentState]
        t.term shouldBe 1
        t.id shouldBe clusterConfig.myId
        List("Leader", "Candidate")  should contain(t.mode)
      }

      eventually(
        timeout(scaled(electionTimeout * 2)),
        interval(scaled(2 seconds))
      ) {
        r ! Raft.GetState(probe.ref)
        probe.expectMessage(
          Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Leader")
        )
      }

      eventually(timeout(scaled(electionTimeout)), interval(scaled(1 seconds))) {
        val t =
          monitorProbe.expectMessageType[Raft.AppendEntries]
        t.leader shouldBe clusterConfig.myId
        t.term shouldBe 1
      }

      eventually(timeout(scaled(electionTimeout)), interval(scaled(1 seconds))) {
        val t =
          monitorProbe.expectMessageType[Raft.AppendEntries]
        t.leader shouldBe clusterConfig.myId
        t.term shouldBe 1
      }

    }

    "timeout without majority votes in leader role" in {

      implicit val clusterConfig: Cluster = new Cluster {

        def otherMembers = Set(ServerId(20))
        override def members: Set[ServerId] = otherMembers + myId

        override def memberRefs =
          otherMembers.map { member =>
            (member, spawn(behavior = Behaviors.receive[Raft.RaftCmd] {
              (_, cmd) =>
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
            }))
          }.toMap

        override val myId: ServerId = ServerId((new Random()).nextInt() & Integer.MAX_VALUE)
      }

      val r = spawn(raft.Raft())

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)
      eventually(timeout(scaled(5 seconds)), interval(scaled(5 millis))) {
        r ! Raft.GetState(probe.ref)
        probe.expectMessage(
          Raft
            .CurrentState(id = clusterConfig.myId, term = 1, mode = "Candidate")
        )
      }

      manualTime.timePasses(electionTimeout)

      eventually(timeout(scaled(5 seconds)), interval(scaled(5 millis))) {
        r ! Raft.GetState(probe.ref)
        probe.expectMessage(
          Raft
            .CurrentState(id = clusterConfig.myId, term = 2, mode = "Candidate")
        )
      }

    }

    "transition from leader to follower on seeing higher term" in {
      val monitorProbe = testKit.createTestProbe[RaftCmd]()

      implicit val clusterConfig: Cluster = new Cluster {

        def otherMembers = Set(ServerId(40), ServerId(50))
        override def members: Set[ServerId] = otherMembers + myId

        override def memberRefs =
          otherMembers.map { member =>
            (
              member,
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
            )
          }.toMap

        override val myId: ServerId = ServerId((new Random()).nextInt() & Integer.MAX_VALUE)
      }

      val r = spawn(raft.Raft())

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)
      r ! Raft.GetState(probe.ref)
      probe.fishForMessage(electionTimeout * 4) {
        case Raft.CurrentState(id, term, _)
            if term != 1 || id != clusterConfig.myId =>
          FishingOutcomes.fail("Got message with wrong term or id")
        case Raft.CurrentState(_, _, "Candidate") => {
          r ! Raft.GetState(probe.ref)
          FishingOutcomes.continue
        }
        case Raft.CurrentState(_, _, "Leader") => FishingOutcomes.complete
        case _ => {
          r ! Raft.GetState(probe.ref)
          FishingOutcomes.continueAndIgnore
        }
      }

      /* Should see heartbeats */
      eventually(timeout(scaled(electionTimeout)), interval(scaled(1 seconds))) {
        val t =
          monitorProbe.expectMessageType[Raft.AppendEntries]
        t.leader shouldBe clusterConfig.myId
        t.term shouldBe 1
      }

      r ! Raft.RequestVote(2, ServerId(50), LogIndex(), monitorProbe.ref)

      eventually(
        timeout(scaled(electionTimeout * 2)),
        interval(scaled(2 seconds))
      ) {
        r ! Raft.GetState(probe.ref)
        probe.expectMessage(
          Raft
            .CurrentState(id = clusterConfig.myId, term = 2, mode = "Follower")
        )
      }

    }

    "return previously set value" in {

      val monitorProbe = testKit.createTestProbe[RaftCmd]()

      implicit val clusterConfig: Cluster = new Cluster {

        def otherMembers = Set(ServerId(200), ServerId(300))
        override def members: Set[ServerId] = otherMembers + myId

        override def memberRefs =
          otherMembers.map { member =>
            (
              member,
              spawn(
                behavior = Behaviors.monitor(
                  monitor = monitorProbe.ref,
                  behavior = Behaviors.receive[Raft.RaftCmd] { (_, cmd) =>
                    cmd match {
                      case RequestVote(term, candidate, _, replyTo) =>
                        replyTo ! RaftReply(term, member, Some(candidate), true)
                        Behaviors.same
                      case req: RaftCmdWithTermExpectingReply =>
                        req.replyTo ! RaftReply(req.term, member, None, true)
                        Behaviors.same
                      case _ =>
                        Behaviors.same
                    }
                  }
                )
              )
            )
          }.toMap

        override val myId: ServerId = ServerId((new Random()).nextInt() & Integer.MAX_VALUE)
      }

      val r = spawn(raft.Raft(), "SuccessSet")

      r ! Raft.GetState(probe.ref)
      probe.expectMessage(Raft.CurrentState(clusterConfig.myId, 0, "Follower"))

      manualTime.timePasses(electionTimeout)
      eventually(
        timeout(scaled(electionTimeout * 2)),
        interval(scaled(2 seconds))
      ) {
        r ! Raft.GetState(probe.ref)
        val t = probe.expectMessageType[Raft.CurrentState]
        t.term shouldBe 1
        t.id shouldBe clusterConfig.myId
        List("Leader", "Candidate")  should contain(t.mode)
      }

      eventually(
        timeout(scaled(electionTimeout * 2)),
        interval(scaled(2 seconds))
      ) {
        r ! Raft.GetState(probe.ref)
        probe.expectMessage(
          Raft.CurrentState(id = clusterConfig.myId, term = 1, mode = "Leader")
        )
      }

      eventually(timeout(scaled(electionTimeout)), interval(scaled(1 seconds))) {
        val t =
          monitorProbe.expectMessageType[Raft.AppendEntries]
        t.leader shouldBe clusterConfig.myId
        t.term shouldBe 1
      }

      eventually(timeout(scaled(electionTimeout)), interval(scaled(1 seconds))) {
        val t =
          monitorProbe.expectMessageType[Raft.AppendEntries]
        t.leader shouldBe clusterConfig.myId
        t.term shouldBe 1
      }

      val clientprobe = createTestProbe[Raft.ClientReply]()

      r ! Raft.SetValue(10, clientprobe.ref)
      clientprobe.expectMessage(Raft.ValueIs(10))

      r ! Raft.GetValue(clientprobe.ref)
      clientprobe.expectMessage(Raft.ValueIs(10))

    }

  }
}
