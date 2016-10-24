package raft

import scala.concurrent.duration._
import akka.actor._
import akka.persistence._
import com.typesafe.config._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

object RaftServer {

  type ServerId = Int

  type Term = Int

  type Index = Int

  abstract case class LogEntry(term: Term)
  case class State(currentTerm: Term = 0, votedFor: Option[ServerId] = None, log: Array[LogEntry] = Array())

  abstract class TermEvent { val term: Term }
  case class SwitchToFollower(term: Term, leader: Option[ServerId]) extends TermEvent
  case class SwitchToCandidate(term: Term) extends TermEvent

  case class AppendEntries(term: Term, leader: ServerId, prevLogIndex: Index, prevLogTerm: Term,
                           entries: Array[LogEntry] = Array(), leaderCommitIndex: Index)

  case class RequestVote(term: Term, candidate: ServerId, lastLogIndex: Index, lastLogTerm: Term)
  case class Response(term: Term, id: ServerId, result: Boolean)

  case class Server(id: ServerId, address: String)

  case class ElectionTimedout()
  case class Heartbeat()
}

import RaftServer._

class RaftServer extends PersistentActor with ActorLogging {

  lazy val raftConfig: Config = context.system.settings.config.atKey("raft")

  lazy val raftServers = raftConfig.getObjectList("servers").asScala.toList
    .map {_.toConfig()}
    .map { srvr => Server(srvr.getInt("id"), srvr.getString("address"))}


  lazy val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

  lazy val myId: ServerId = raftConfig.getInt("myId")
  override def persistenceId = "raft-server-" + myId

  var state = State()

  val handleEvent: PartialFunction[Any, Unit] = {

    case SwitchToFollower(newTerm, leader) => {
      state = state.copy(currentTerm = newTerm)
      /* If the leader fails to send heartbeat within the election timeout
       the server switches to candidate
       */
      context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimedout())
      context.become(follower)
    }

    case SwitchToCandidate(term) => {
      state = state.copy(currentTerm = term)
      context.actorSelection("../*") !  RequestVote(term, myId, 0, 0)
    }
  }

  val receiveCommand: Receive = {
    case RecoveryCompleted =>
      /* A server always starts as a follower, waiting for heartbeat(s)
       from the leader.
       */
      handleEvent(SwitchToFollower(state.currentTerm, None))
  }

  val leader: Receive = {
    case Heartbeat =>
      context.actorSelection("../*") ! AppendEntries(state.currentTerm, myId, 0, 0, Array(), 0)
  }

  def candidate(voters: List[ServerId]): Receive = {
    case Response(term, server, successful) if term > state.currentTerm =>
      persist(SwitchToFollower(term, Some(server)))(handleEvent)
      saveSnapshot(state)
    case Response(term, server, successful) if successful =>
      val newVoters = server :: voters
      if (newVoters.length > raftServers.length / 2) {
        context.system.scheduler.scheduleOnce(electionTimeout/2, self, Heartbeat())
        context.become(leader)
      }
      else
        context.become(candidate(newVoters))
  }

  val follower: Receive = {
    case ElectionTimedout =>
      log.info(s"Failed to receive heartbeat within {electionTimeout}.")
      persist(SwitchToCandidate(state.currentTerm + 1))(handleEvent)
      // TODO: Should we call saveSnapshot here or in the handleEvent
      saveSnapshot(state)
      context.become(candidate(List()))

    case AppendEntries(term, leader, _, _, Array(), _) if term == state.currentTerm =>
      log.debug(s"heartbeat received for {term} from {leader}")
      sender() ! Response(state.currentTerm, myId, true)

    case AppendEntries(term, leader, _, _, Array(), _) if term > state.currentTerm =>
      log.info(s"New leader {leader} with {term}. Switching to follower.")
      persist(SwitchToFollower(term, Some(leader)))(handleEvent)
      // TODO: Should we call saveSnapshot here or in the handleEvent
      saveSnapshot(state)
      sender() ! Response(state.currentTerm, myId, true)

    case RequestVote(term, candidate, lastLogIndex, lastLogTerm) if term < state.currentTerm =>
      sender() ! Response(state.currentTerm, myId, false)

    case RequestVote(term, candidate, lastLogIndex, lastLogTerm) if state.votedFor.getOrElse(candidate) == candidate =>
      sender() ! Response(state.currentTerm, myId, true)
  }

  val receiveRecover: Receive = {
    case evt: TermEvent => state = state.copy(currentTerm = evt.term)
    case SnapshotOffer(_, snapshot: State) => state = snapshot
  }

}

