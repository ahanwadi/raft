package raft

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import RaftServer1._
import akka.actor._
import akka.persistence.fsm.PersistentFSM
import akka.persistence.fsm.PersistentFSM._
import akka.event.LoggingReceive
import akka.persistence._
import com.typesafe.config._
import scala.reflect._

object RaftServer1 {

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
}

class Server extends Actor with PersistentFSM[RaftState, ServerData, TermEvent] {

  val raftConfig: Config = context.system.settings.config.getConfig("raft")

  lazy val raftServers = raftConfig.getObjectList("servers").asScala.toList
    .map {_.toConfig()}
    .map { srvr => Server(srvr.getInt("id"), srvr.getString("address"))}

  lazy val quorumSize = raftServers.length/2 + 1

  lazy val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

  lazy val heartbeatInterval: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("heartbeat-interval").toNanos())

  lazy val myId: ServerId = raftConfig.getInt("myId")

  override def persistenceId = "raft-server-" + myId

  override def domainEventClassTag: ClassTag[TermEvent] = classTag[TermEvent]

  log.info(s"Initialization complete with $persistenceId and timeout of ${electionTimeout}")

  startWith(Follower, ServerData())

  when(Follower, stateTimeout = electionTimeout) {
    case Event(StateTimeout, rs: ServerData) =>
      goto(Candidate) applying SwitchToCandidate() andThen {
        case d: ServerData => {
          saveStateSnapshot
          context.actorSelection("../*") !  RequestVote(d.currentTerm, myId)
        }
      }

    case Event(AppendEntries(term, leader), ServerData(currentTerm, _, _)) if term < currentTerm =>
      stay replying HeartbeatResponse(currentTerm, myId)

    case Event(RequestVote(term, candidate), ServerData(currentTerm, votedFor, _)) if term < currentTerm =>
      stay replying Vote(currentTerm, myId, votedFor, false)

    case Event(AppendEntries(term, leader), ServerData(currentTerm, votedFor, _)) => {
      // vote applies only to a given term.
      val newVotedFor = if (term > currentTerm) None else votedFor
      stay applying SwitchToFollower(term, newVotedFor) replying HeartbeatResponse(term, myId)
    }

    case Event(RequestVote(term, candidate), ServerData(currentTerm, votedFor, _)) => {
      // vote applies only to a given term.
      val actualVotedFor = if (term > currentTerm) Some(candidate) else votedFor.orElse(Some(candidate))
      val successful = Some(candidate) == votedFor && term >= currentTerm
      goto(Follower) applying SwitchToFollower(term, actualVotedFor) replying Vote(term, myId, actualVotedFor, successful)
    }

  }

  when(Candidate) {
    case Event(StateTimeout, rs: ServerData) =>
      goto(Candidate) applying SwitchToCandidate() andThen {
        case d: ServerData => {
          saveStateSnapshot
          context.actorSelection("../*") !  RequestVote(d.currentTerm, myId)
        }
      }

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, _)) if term < currentTerm =>
      stay

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, _)) if term > currentTerm =>
      goto(Follower) applying SwitchToFollower(term, None)

    case Event(Vote(term, serverId, vote, successful), ServerData(currentTerm, votedFor, _)) if (successful && vote == Some(myId)) =>
      stay applying GotVote(serverId) andThen {
        case d: ServerData => {
          saveStateSnapshot
          if (d.votes.size == quorumSize)
            goto(Leader)
        }
      }
  }

  when(Leader, stateTimeout = electionTimeout) {
    case Event(Heartbeat, state: ServerData) =>
      context.actorSelection("../*") ! AppendEntries(state.currentTerm, myId)
      stay

    case Event(HeartbeatResponse(term, server), state: ServerData) if term > state.currentTerm =>
      stay applying SwitchToFollower(term, None)
  }

  onTransition {
    case _ -> Leader =>
      setTimer("heartBeat", Heartbeat, heartbeatInterval, repeat = true)

    case Leader -> _ =>
      cancelTimer("heartBeat")
  }


  override def applyEvent(event: TermEvent, stateBefore: ServerData): ServerData = {
    event match {
      case SwitchToCandidate() => stateBefore.copy(currentTerm = stateBefore.currentTerm + 1, votedFor = Some(myId))
      case SwitchToFollower(newTerm, leader) => stateBefore.copy(currentTerm = newTerm, votedFor = leader)
      case GotVote(voter) => stateBefore.copy(votes = stateBefore.votes + voter)
    }
  }
}
