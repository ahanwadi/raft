package raft

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import RaftFSM._
import akka.actor._
import akka.persistence.fsm.PersistentFSM
import akka.persistence.fsm.PersistentFSM._
import akka.event.LoggingReceive
import akka.persistence._
import com.typesafe.config._
import scala.reflect._
import java.util.concurrent.ThreadLocalRandom

object RaftFSM {

  type ServerId = Int

  type Term = Int

  type Index = Int

  // The persistent state stored by all servers.
  case class ServerData(currentTerm: Term = 0, votedFor: Option[ServerId] = None, votes: Set[ServerId] = Set())

  sealed trait RaftState extends FSMState

  case object Follower extends RaftState {
    override def identifier: String = "Follower"
  }

  case object Leader extends RaftState {
    override def identifier: String = "Leader"
  }

  case object Candidate extends RaftState {
    override def identifier: String = "Candidate"
  }

  abstract class TermEvent
  case class SwitchToFollower(term: Term, leader: Option[ServerId] = None) extends TermEvent
  case class SwitchToCandidate() extends TermEvent
  case class GotVote(voter: ServerId) extends TermEvent

  /* RPC requests and Responses */
  case class AppendEntries(term: Term, leader: ServerId)
  case class HeartbeatResponse(term: Term, id: ServerId)

  case class RequestVote(term: Term, candidate: ServerId)
  case class Vote(term: Term, server: ServerId, votedFor: Option[ServerId], result: Boolean)

  case class Server(id: ServerId, address: String)

  object ElectionTimedout
  object Heartbeat
  object GetState
  object IsLeader
}

class RaftFSM(val myId: ServerId) extends Actor with PersistentFSM[RaftState, ServerData, TermEvent] {

  val raftConfig: Config = context.system.settings.config.getConfig("raft")

  lazy val raftServers = raftConfig.getObjectList("servers").asScala.toList
    .map {_.toConfig()}
    .map { srvr => Server(srvr.getInt("id"), srvr.getString("address"))}

  lazy val quorumSize = raftServers.length/2 + 1

  private val rand: util.Random = new util.Random(compat.Platform.currentTime)

  lazy val cfgElectTimeout = raftConfig.getDuration("election-timeout").toMillis()

  private def electionTimeout(): FiniteDuration = {
    Duration(ThreadLocalRandom.current().nextLong(cfgElectTimeout - 1, cfgElectTimeout * 2), MILLISECONDS)
  }

  lazy val heartbeatInterval: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("heartbeat-interval").toNanos())

  override def persistenceId = "raft-server-" + myId

  override def domainEventClassTag: ClassTag[TermEvent] = classTag[TermEvent]

  log.info(s"Initialization complete with $persistenceId and timeout of ${electionTimeout} with quorumSize of ${quorumSize}")

  startWith(Follower, ServerData())

  when(Follower, stateTimeout = electionTimeout) {

    case Event(ElectionTimedout, rs: ServerData) =>
      log.debug("Timedout in follower state ")
      goto(Candidate) applying SwitchToCandidate()

    case Event(AppendEntries(term, leader), ServerData(currentTerm, _, _)) => {
      goto(Follower) replying HeartbeatResponse(term, myId)
    }

    case Event(RequestVote(term, candidate), ServerData(currentTerm, votedFor, _)) if term == currentTerm => {
      // vote applies only to a given term.
      val alreadyVoted = Some(candidate) == votedFor
      val actualVotedFor = votedFor.orElse(Some(candidate))
      val successful = actualVotedFor == Some(candidate)
      if (!successful) {
        stay replying Vote(term, myId, votedFor, false)
      } else if (alreadyVoted) {
        log.info(s"Already voted for ${candidate} in term ${term}")
        goto(Follower) replying Vote(term, myId, votedFor, true)
      } else {
        log.info(s"Voted for ${candidate} in term ${term}")
        goto(Follower) applying SwitchToFollower(term, actualVotedFor) replying Vote(term, myId, actualVotedFor, true)
      }
    }

  }

  when(Candidate) {
    case Event(ElectionTimedout, rs: ServerData) =>
      log.debug("Election timedout")
      goto(Candidate) applying SwitchToCandidate()

    case Event(RequestVote(term, candidate), ServerData(currentTerm, votedFor, _)) if term == currentTerm => {
      val successful = myId == candidate
      if (!successful) {
        stay replying Vote(term, myId, votedFor, false)
      } else {
        log.info(s"Voted for ${candidate} in term ${term}")
        stay replying Vote(term, myId, votedFor, true)
      }
    }

    case Event(AppendEntries(term, leader), ServerData(currentTerm, _, _)) => {
      goto(Follower) replying HeartbeatResponse(term, myId)
    }

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, votes)) if (successful && vote == Some(myId)) =>
      log.info(s"Received vote from ${serverId} in term ${term}")
      val newVotes = votes + serverId
      if (newVotes.size == quorumSize) {
        log.debug("Switching to leader")
        goto(Leader) applying GotVote(serverId)
      } else
        stay applying GotVote(serverId)
  }

  whenUnhandled {
    case Event(GetState, s) =>
      stay replying s

    case Event(IsLeader, s) =>
      stay replying s"${stateName}"

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, _)) if term < currentTerm =>
      stay

    case Event(RequestVote(term, serverId), ServerData(currentTerm, votedFor, _)) if term < currentTerm =>
      stay replying Vote(term, myId, votedFor, false)

    case Event(AppendEntries(term, leader), ServerData(currentTerm, _, _)) if term < currentTerm =>
      stay replying HeartbeatResponse(currentTerm, myId)

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, _)) if term > currentTerm =>
      goto(Follower) applying SwitchToFollower(term, None)

    case Event(RequestVote(term, candidate), ServerData(currentTerm, votedFor, _)) if term > currentTerm =>
      goto(Follower) applying SwitchToFollower(term, Some(candidate)) replying Vote(term, myId, Some(candidate), true)

    case Event(AppendEntries(term, serverId), ServerData(currentTerm, _, _)) if term > currentTerm =>
      goto(Follower) applying SwitchToFollower(term, None)

    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  when(Leader) {
    case Event(Heartbeat, state: ServerData) =>
      context.actorSelection("../*") ! AppendEntries(state.currentTerm, myId)
      stay

    case Event(HeartbeatResponse(term, server), state: ServerData) if term > state.currentTerm =>
      goto(Follower) applying SwitchToFollower(term, None)
  }

  onTransition {
    case _ -> Follower =>
      cancelTimer("election")
      setTimer("election", ElectionTimedout, electionTimeout, repeat = false)
      log.debug(s"Switching to follower: ${nextStateData}")

    case _ -> Candidate =>
      cancelTimer("election")
      val newElectionTimeout = electionTimeout()
      setTimer("election", ElectionTimedout, newElectionTimeout, repeat = false)
      log.debug(s"Switching to candidate: ${nextStateData} with timeout of ${newElectionTimeout}")
      context.actorSelection("../*") !  RequestVote(nextStateData.currentTerm, myId)

    case _ -> Leader =>
      cancelTimer("election")
      log.debug(s"Elected leader: ${nextStateData}")
      setTimer("heartBeat", Heartbeat, heartbeatInterval, repeat = true)

    case Leader -> _ =>
      cancelTimer("heartBeat")
  }

  override def applyEvent(event: TermEvent, stateBefore: ServerData): ServerData = {
    event match {
      case SwitchToCandidate() => {
        log.debug(s"Applying SwitchToCandidate")
        stateBefore.copy(currentTerm = stateBefore.currentTerm + 1, votedFor = Some(myId), votes = stateBefore.votes + myId)
      }
      case SwitchToFollower(newTerm, leader) => stateBefore.copy(currentTerm = newTerm, votedFor = leader, votes = Set())
      case GotVote(voter) => stateBefore.copy(votes = stateBefore.votes + voter)
    }
  }
}
