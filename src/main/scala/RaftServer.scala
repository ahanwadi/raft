package raft

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import RaftServer._
import akka.actor._
import akka.event.LoggingReceive
import akka.persistence._
import com.typesafe.config._

object RaftServer {

  type ServerId = Int

  type Term = Int

  type Index = Int

  // The persistent state stored by all servers.
  case class State(currentTerm: Term = 0, votedFor: Option[ServerId] = None)

  abstract class TermEvent
  case class SwitchToFollower(term: Term, leader: Option[ServerId] = None) extends TermEvent
  case class SwitchToCandidate() extends TermEvent
  case class AppendEntries(term: Term, leader: ServerId)
  case class HeartbeatResponse(term: Term, id: ServerId)

  case class RequestVote(term: Term, candidate: ServerId)
  case class Vote(term: Term, server: ServerId, votedFor: Option[ServerId], result: Boolean)

  case class Server(id: ServerId, address: String)

  object ElectionTimedout
  object Heartbeat
  object GetState
}

/**
  * Raft is a consensus protocol for building distributed systems based on
  * replicated state machines.
  */
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
      log.info(s"${myId} becoming follower for ${leader}")
      persist(SwitchToFollower(term, leader))(handleEvent)
      saveSnapshot(state)
    }

    val s = context.self

    /* If the leader fails to send heartbeat within the election timeout
     the server switches to candidate
     */

    val cb1 = cb.getOrElse {
      context.system.scheduler.scheduleOnce(electionTimeout) {
        log.info("Got Election timeout")
        s ! ElectionTimedout
      }
    }

    log.debug(s"${myId} staying follower for ${state.votedFor}")
    context.become(follower(cb1))
  }

  def switchToCandidate(cb: Option[Cancellable] = None) {
    cb.map(_.cancel())

    /*
     * We need to persist this as we implicitly vote for ourselves and
     * increment the term.
     */
    persist(SwitchToCandidate())(handleEvent)
    saveSnapshot(state)
    context.actorSelection("../*") !  RequestVote(state.currentTerm, myId)

    val cb1 = cb.getOrElse {
      context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimedout)
    }

    context.become(candidate(cb1))
  }

  def switchToLeader(cb: Option[Cancellable] = None) {
    cb.map(_.cancel())
    context.actorSelection("../*") ! AppendEntries(state.currentTerm, myId)
    val cb1 = cb.getOrElse {
      context.system.scheduler.scheduleOnce(electionTimeout/2, self, Heartbeat)
    }
    context.become(leader(cb1))
  }


  val handleEvent: PartialFunction[Any, Unit] = {

    /* We need to save both the term and votedFor as we are allowed to vote for only one candidate per term */
    case SwitchToFollower(newTerm, leader) => {
      state = state.copy(currentTerm = newTerm, votedFor = leader)
    }

    /* We only switch from follower to candidate only when the leader for the current term
     * fails to heartbeat within the timeout period. Since, servers vote for at most one candidate in any given
     * term, we have to increment the term and vote for ourselves
     */
    case SwitchToCandidate() => {
      state = state.copy(currentTerm = state.currentTerm + 1, votedFor = Some(myId))
    }

  }

  val receiveCommand: Receive = LoggingReceive.withLabel("general") {
    case GetState =>
      log.debug(s"Got getState: ${state.currentTerm}, ${state.votedFor}")
      sender() ! (state.currentTerm, state.votedFor)
  }

  /**
    * A leader for a given term accepts client requests, replicates logs to a quorum of
    * followers before responding to clients.
    */
  def leader(cb: Cancellable): Receive = {
    case Heartbeat =>
      // Send heartbeat to all servers
      switchToLeader()

    case HeartbeatResponse(term, server) if term == state.currentTerm =>
      log.info("Got heartbeat response from ${server} for ${term}")

    case HeartbeatResponse(term, server) if term > state.currentTerm =>
      // Just update the term only
      switchToFollower(term, None, Some(cb))

    case z =>
      log.info(s"$z")
  }

  def candidate(cb: Cancellable, voters: Set[ServerId] = Set(myId)): Receive = LoggingReceive.withLabel("candidate") {

    case ElectionTimedout =>
      log.error(s"Failed to receive votes within the timeout for the ${state.currentTerm}")
      // Start a new election
      switchToCandidate()

    /* Stale term */
    case RequestVote(term, candidate) if term < state.currentTerm =>
      // Reject vote, let the sender know about the current term.
      sender() ! Vote(state.currentTerm, myId, state.votedFor, false)

    /* Discovers new term */
    case RequestVote(term, candidate) if term > state.currentTerm =>
      // Vote for the candidate and switch to the follower mode.
      switchToFollower(term, Some(candidate), Some(cb))
      sender() ! Vote(state.currentTerm, myId, state.votedFor, true)

    case RequestVote(term, candidate) =>
      // If request from myself, wait for other votes. If request from another
      // candidate, reject it.
      sender() ! Vote(state.currentTerm, myId, state.votedFor, candidate == myId)

    // Discovered a new term.
    case Vote(term, server, votedFor, successful) if term > state.currentTerm =>
      assert(!successful)
      switchToFollower(term, None, Some(cb))

    // Got a new vote.
    case Vote(term, server, votedFor, successful) if successful && term == state.currentTerm =>
      assert(votedFor == Some(myId))
      val newVoters: Set[ServerId] = voters + server
      if (newVoters.size > raftServers.length / 2) {
        // Got votes from majority of servers.
        switchToLeader(Some(cb))
      } else {
        context.become(candidate(cb, newVoters))
      }

    // Stale heartbeat
    case AppendEntries(term, leader) if term < state.currentTerm =>
      sender() ! HeartbeatResponse(state.currentTerm, myId)

    // Discovered a new leader.
    case AppendEntries(term, leader) if term >= state.currentTerm =>
      log.info(s"New leader ${leader} with ${term}. Switching to follower.")
      val l = if (term == state.currentTerm) state.votedFor else None
      // There is no need to vote since we are simply discovering existing leader.
      switchToFollower(term, l, Some(cb))
      sender() ! HeartbeatResponse(term, myId)

    // TODO - put it into common receiver method.
    case GetState =>
      log.info(s"getState: sending ${state.currentTerm}, ${state.votedFor}")
      sender() ! (state.currentTerm, state.votedFor)

    case z =>
      log.info(s"Got message: ${z}")
  }

  /*
   * A server stays in follower state until it timeouts (-> candidate term+1) without
   * receiving any communication from follower or a leader.
   * This state should be split into learner and follower.
   */
  def follower(cb: Cancellable): Receive = LoggingReceive.withLabel("follower") {
    case ElectionTimedout =>
      log.info(s"Failed to receive heartbeat within ${electionTimeout}.")
      switchToCandidate()

    // Stale leader
    case AppendEntries(term, leader) if term < state.currentTerm =>
      log.debug(s"stale heartbeat received for ${term} from ${leader}")
      sender() ! HeartbeatResponse(state.currentTerm, myId)

    // Discovered current leader.
    case AppendEntries(term, leader) if term == state.currentTerm =>
      log.debug(s"heartbeat received for ${term} from ${leader}")
      // Restart the election timer
      switchToFollower(term, state.votedFor, Some(cb))
      sender() ! HeartbeatResponse(term, myId)

    // Discovered new term and a leader
    case AppendEntries(term, leader) if term > state.currentTerm =>
      log.info(s"New leader ${leader} with ${term}. Switching to follower.")
      switchToFollower(term, None, Some(cb))
      sender() ! HeartbeatResponse(state.currentTerm, myId)

    // Stale candidate/election.
    case RequestVote(term, candidate) if term < state.currentTerm =>
      sender() ! Vote(state.currentTerm, myId, state.votedFor, false)

    // Reject a vote from another candidate.
    case RequestVote(term, candidate) if term == state.currentTerm && state.votedFor.getOrElse(candidate) != candidate =>
      sender() ! Vote(state.currentTerm, myId, state.votedFor, false)

    /*
     * Be loyal.
     */
    case RequestVote(term, candidate) if term == state.currentTerm && state.votedFor.orElse(Some(candidate)) == candidate =>
      switchToFollower(term, Some(candidate), Some(cb))
      sender() ! Vote(state.currentTerm, myId, state.votedFor, true)

    /*
     * A new term begins. Vote the candidate.
     */
    case RequestVote(term, candidate) if term > state.currentTerm =>
      switchToFollower(term, Some(candidate), Some(cb))
      sender() ! Vote(state.currentTerm, myId, state.votedFor, true)

    case GetState =>
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
