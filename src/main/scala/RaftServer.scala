package raft

import scala.concurrent.duration._
import akka.actor._
import akka.persistence._
import com.typesafe.config._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.event.LoggingReceive

object RaftServer {

  type ServerId = Int

  type Term = Int

  type Index = Int

  abstract case class LogEntry(term: Term)
  case class State(currentTerm: Term = 0, votedFor: Option[ServerId] = None)

  abstract class TermEvent
  case class SwitchToFollower(term: Term, leader: Option[ServerId] = None) extends TermEvent
  case class SwitchToCandidate() extends TermEvent
  case class AppendEntries(term: Term, leader: ServerId)

  case class RequestVote(term: Term, candidate: ServerId)
  case class Response(term: Term, id: ServerId, result: Boolean)

  case class Server(id: ServerId, address: String)

  case class ElectionTimedout()
  case class Heartbeat()
}

import RaftServer._

class RaftServer extends PersistentActor with ActorLogging {

  val raftConfig: Config = context.system.settings.config.getConfig("raft")

  lazy val raftServers = raftConfig.getObjectList("servers").asScala.toList
    .map {_.toConfig()}
    .map { srvr => Server(srvr.getInt("id"), srvr.getString("address"))}

  lazy val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

  lazy val myId: ServerId = raftConfig.getInt("myId")
  override def persistenceId = "raft-server-" + myId

  log.info(s"Initialization complete with $persistenceId and timeout of ${electionTimeout}")

  var state = State()

  def switchToFollower(term: Term, leader: Option[ServerId] = None, cb: Option[Cancellable] = None) {
    cb.map(_.cancel())

    if (term > state.currentTerm || leader != state.votedFor) {
      persist(SwitchToFollower(state.currentTerm, leader))(handleEvent)
      saveSnapshot(state)
    }

    val s = context.self
    /* If the leader fails to send heartbeat within the election timeout
     the server switches to candidate
     */
    val cb1 = context.system.scheduler.scheduleOnce(electionTimeout) {
      log.info("Got Election timeout")
      s ! ElectionTimedout()
    }

    log.info(s"${s} becoming follower")
    context.become(follower(cb1))
  }

  def switchToCandidate(cb: Option[Cancellable] = None) {
    cb.map(_.cancel())

    persist(SwitchToCandidate())(handleEvent)
    saveSnapshot(state)
    context.actorSelection("../*") !  RequestVote(state.currentTerm, myId)
    val cb1 = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimedout())
    context.become(candidate(Set(myId), cb1))
  }

  def switchToLeader(cb: Option[Cancellable] = None) {
    cb.map(_.cancel())
    context.actorSelection("../*") ! AppendEntries(state.currentTerm, myId)
    val cb1 = context.system.scheduler.scheduleOnce(electionTimeout/2, self, Heartbeat())
    context.become(leader(cb1))
  }

  val handleEvent: PartialFunction[Any, Unit] = {

    /* We need to save both the term and votedFor as we are allowed to vote for only one candidate per term */
    case SwitchToFollower(newTerm, leader) => {
      state = state.copy(currentTerm = newTerm, votedFor = leader)
    }

    /* We only switch from follower to candidate only when the leader for the current term
     fails to heartbeat within the timeout period. Since, servers vote for at most one candidate in any given
     term, we have to increment the term and vote for ourselves */
    case SwitchToCandidate() => {
      state = state.copy(currentTerm = state.currentTerm + 1, votedFor = Some(myId))
    }

  }

  val receiveCommand: Receive = LoggingReceive.withLabel("testing") {
    case "getState" => log.debug(s"Got getState: ${state.currentTerm}, ${state.votedFor}"); sender() ! (state.currentTerm, state.votedFor)
  }

  def leader(cb: Cancellable): Receive = {
    case Heartbeat =>
      switchToLeader(Some(cb))

    case Response(term, server, success) if term > state.currentTerm =>
      switchToFollower(term, Some(server), Some(cb))

    case Response(term, server, successful) if term == state.currentTerm && successful =>
      log.info(s"Successful heartbeat response from {server} for the {term}")

    case z =>
      log.info(s"$z")
  }

  def candidate(voters: Set[ServerId] = Set(myId), cb: Cancellable): Receive = LoggingReceive.withLabel("candidate") {

    case x: ElectionTimedout =>
      log.error(s"Failed to receive votes within the timeout for the ${state.currentTerm}")
      switchToCandidate(Some(cb))

    case RequestVote(term, candidate) if term == state.currentTerm && state.votedFor.isEmpty =>
      sender() ! Response(state.currentTerm, myId, true)

    case RequestVote(term, candidate) if term > state.currentTerm =>
      switchToFollower(term, Some(candidate), Some(cb))
      sender() ! Response(state.currentTerm, state.votedFor.get, true)

    case Response(term, server, successful) if term > state.currentTerm =>
      switchToFollower(term, Some(server), Some(cb))

    case Response(term, server, successful) if successful && term == state.currentTerm =>
      val newVoters: Set[ServerId] = voters + server
      if (newVoters.size > raftServers.length / 2) {
        switchToLeader(Some(cb))
      } else {
        context.become(candidate(newVoters, cb))
      }

    case AppendEntries(term, leader) if term < state.currentTerm =>
      sender() ! Response(state.currentTerm, state.votedFor.getOrElse(myId), false)

    case AppendEntries(term, leader) if term >= state.currentTerm =>
      log.info(s"New leader ${leader} with ${term}. Switching to follower.")
      switchToFollower(term, Some(leader), Some(cb))
      sender() ! Response(state.currentTerm, myId, true)

    case (x: ServerId, y: Option[ServerId]) =>
      log.info("Skip")

    case "getState" =>
      log.info(s"getState: sending ${state.currentTerm}, ${state.votedFor}")
      sender() ! (state.currentTerm, state.votedFor)

    case z =>
      log.info(s"Got message: ${z}")
  }

  /* This state should be split into learner and follower.
   A server stays in listener until it timeouts (-> candidate term+1) or gets heartbeat from the leader (-> follower term of the leader).
   */
  def follower(cb: Cancellable): Receive = LoggingReceive.withLabel("other") {
    case x: ElectionTimedout =>
      log.info(s"Failed to receive heartbeat within ${electionTimeout}.")
      switchToCandidate(Some(cb))

    case AppendEntries(term, leader) if term < state.currentTerm =>
      log.debug(s"stale heartbeat received for ${term} from ${leader}")
      sender() ! Response(state.currentTerm, myId, false)

    case AppendEntries(term, leader) if term == state.currentTerm =>
      log.debug(s"heartbeat received for ${term} from ${leader}")
      switchToFollower(term, Some(leader), Some(cb))
      sender() ! Response(state.currentTerm, myId, true)

    case AppendEntries(term, leader) if term > state.currentTerm =>
      log.info(s"New leader ${leader} with ${term}. Switching to follower.")
      switchToFollower(term, Some(leader), Some(cb))
      sender() ! Response(state.currentTerm, myId, true)

    case RequestVote(term, candidate) if term < state.currentTerm =>
      sender() ! Response(state.currentTerm, myId, false)

    case RequestVote(term, candidate) if term >= state.currentTerm || state.votedFor.getOrElse(candidate) == candidate =>
      switchToFollower(term, Some(candidate), Some(cb))
      sender() ! Response(state.currentTerm, myId, true)

    case "getState" =>
      log.info(s"getState: sending ${state.currentTerm}, ${state.votedFor}")
      sender() ! (state.currentTerm, state.votedFor)

    case y =>
      log.info(s"Got generic message: $y")
  }

  val receiveRecover: Receive = {
    case RecoveryCompleted =>
      /* A server always starts as a follower, waiting for heartbeat(s)
       from the leader.
       */
      log.info("Recovery completed, switching to follower")
      switchToFollower(state.currentTerm)
    case evt: TermEvent => handleEvent(evt)
    case SnapshotOffer(_, snapshot: State) => state = snapshot
    case x => log.debug(s"Got message in receiveRecover $x")
  }

}
