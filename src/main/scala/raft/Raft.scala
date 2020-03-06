package raft

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.scaladsl.Effect.reply
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import raft.Replicator.LogAppended

import scala.collection.{immutable => im}

/**
  * We use typed akka and akka persistence to model an Raft server.
  * We persist each new term, votes, and logs.
  */
object Raft {

  /** Starts the heartbeat/election timer */
  def startHeartbeatTimer(
                           timers: TimerScheduler[RaftCmd],
                           context: ActorContext[RaftCmd]
                         ): Unit = {
    val electionTimeout = RaftConfig(context.system).electionTimeout
    timers.startSingleTimer(HeartbeatTimeout, HeartbeatTimeout, electionTimeout)
  }

  def apply(id: Option[ServerId] = None)(
    implicit clusterConfig: Cluster
  ): Behavior[RaftCmd] = Behaviors.setup { context: ActorContext[RaftCmd] =>
    val myId: ServerId = id.getOrElse(clusterConfig.myId)
    val persistenceId = PersistenceId.ofUniqueId(s"raft-server-typed-$myId.id")

    Behaviors.withTimers { timers: TimerScheduler[RaftCmd] =>
      EventSourcedBehavior[RaftCmd, Event, State](
        persistenceId = persistenceId,
        emptyState = Follower(myId),
        commandHandler = dispatcher(timers, context, clusterConfig),
        eventHandler = eventHandler(clusterConfig)
      ).snapshotWhen {
        case (_, ConflictingEntries(_, _, _), _) => true
        case _ => false
      }
        .receiveSignal {
          case (state, RecoveryCompleted) =>
            state.enterMode(timers, context, clusterConfig)
        }
    }
  }

  def dispatcher(
                  timers: TimerScheduler[RaftCmd],
                  context: ActorContext[RaftCmd],
                  clusterConfig: Cluster
                ): (Raft.State, RaftCmd) => Effect[Raft.Event, Raft.State] = {
    (state: Raft.State, cmd: RaftCmd) => {
      def secondhandler
      : PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
        case c =>
          state.commandhandler(timers, context, clusterConfig)(state, c)
      }

      val y: Effect[Event, State] =
        state
          .defaultHandler(timers, context = context, clusterConfig)
          .orElse(secondhandler)(cmd)
      y
    }
  }

  def eventHandler(
                    clusterConfig: Cluster
                  ): (Raft.State, Raft.Event) => Raft.State =
    (state: State, evt: Event) => state.eventHandler(clusterConfig, evt)

  /* Election Term */
  sealed trait Term extends Any {
    def term: Int
  }

  /**
    * Base class for a external and internal client commands handled by
    * the leader.
    */
  sealed trait ClientCmd extends RaftCmd

  /** Base class for client replies */
  sealed trait ClientReply extends Any

  /** Base class for all commands logged by the Raft server's RSM */
  sealed trait RSMCmd

  /** Base class for all events that cause state transition.
    * Also all events are persisted in a log for recovery
    */
  sealed trait Event

  /** The persistent state stored by all servers. */
  sealed trait State {

    /* The current value. It can be set by SetCmd and retrieved by GetVal */
    def value: Int = 0

    def eventHandler(clusterConfig: Cluster, evt: Event): State = evt match {
      /*
       * When votedFor == empty or votedFor != myId became follower
       * Else goto Candidate.
       */
      case NewTerm(term, votedFor) if votedFor.contains(myId) =>
        Candidate(myId, term, votedFor, rsm = rsm)

      case NewTerm(term, votedFor) =>
        Follower(myId, term, votedFor, rsm = rsm)

      // TODO - This should be moved to the Follower state
      case ConflictingEntries(Index(idx), term, votedFor) =>
        Follower(
          myId,
          term,
          votedFor,
          rsm.copy(logs = rsm.logs.dropRight(rsm.logs.length - idx))
        )
    }

    /* Whether leader or follower */
    def getMode: String

    /**
      * Handles given command in the context of the current state.
      */
    def commandhandler(
                        timers: TimerScheduler[RaftCmd],
                        context: ActorContext[RaftCmd],
                        clusterConfig: Cluster
                      ): CommandHandler[RaftCmd, Raft.Event, Raft.State]

    /* The unique of the server */
    def myId: ServerId = ServerId(0)

    /* Txn logs for this server */
    def rsm: Logs = Logs()

    /**
      * The default handler for all server types (modes).
      */
    def defaultHandler(
                        timers: TimerScheduler[RaftCmd],
                        context: ActorContext[RaftCmd],
                        clusterConfig: Cluster
                      ): PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
      case cmd@RequestVote(term, candidate, _, _) if term > currentTerm =>
        context.log.info(
          s"Voting for $candidate in $term and transitioning to follower"
        )
        // TODO: Eliminate duplicate code fragments by refactoring this into an utility method.
        Effect
          .persist(NewTerm(term, Some(candidate)))
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, Some(candidate), result = true)
          }
      case cmd@AppendEntries(term, _, _, _, _, _) if term > currentTerm =>
        processLogs(timers, context, clusterConfig, cmd)
      case cmd: RaftCmdWithTermExpectingReply if cmd.term < currentTerm =>
        // Reject all commands with term less than current term
        reply(cmd.replyTo)(RaftReply(currentTerm, myId, None, result = false))
      case e: Term if e.term > currentTerm =>
        Effect.persist(NewTerm(e.term)).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case e: Term if e.term < currentTerm =>
        Effect.none // Ignore stale messages
      case c: GetState =>
        reply(c.replyTo)(CurrentState(myId, currentTerm, getMode))
    }

    /* The current term of this server */
    def currentTerm: Int = 0

    def processLogs(
                     timers: TimerScheduler[RaftCmd],
                     context: ActorContext[RaftCmd],
                     clusterConfig: Cluster,
                     cmd: AppendEntries
                   ): ReplyEffect[Event, State] = {
      val AppendEntries(term, leader, _, prevLog, _, logs) = cmd
      context.log.info(s"Got heartbeat from new leader $leader in $term")
      val prevLogIdx = prevLog.index.idx
      def acceptLogs: ReplyEffect[Event, State] = {
        // Accept new logs
        var idx = prevLogIdx
        var newidx = 0
        var conflict = false
        while (idx < rsm.logs.length && newidx < logs.length && !conflict) {
          if (rsm.logs(idx).term == logs(newidx).term) {
            // Skip ahead
            idx += 1
            newidx += 1
          } else {
            // Conflicting entries, delete all logs from this point onwards.
            conflict = true
          }
        }
        val evt =
          if (conflict)
            ConflictingEntries(Index(idx), term, Some(leader))
          else
            NewTerm(term, Some(leader))

        val evts: im.Seq[Event] = evt +: logs
          .takeRight(logs.length - newidx)
          .to(Seq)
        Effect
          .persist(evts)
          .thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, Some(leader), result = true)
          }
      }
      if (rsm.logs.isEmpty) {
        acceptLogs
      } else if (prevLogIdx < rsm.logs.length) {
        val log = rsm.logs(prevLog.index.idx)
        if (log.term != prevLog.term) {
          // Found matching entry to prevLog with the same index and but conflicting term
          // Reject the AppendEntries request and become a follower.
          context.log.info("Rejecting log entries from $leader at $prevLog")
          reply(cmd.replyTo)(RaftReply(term, myId, Some(leader), result = false))
        } else {
          acceptLogs
        }
      } else {
        context.log.info("Rejecting log entries from $leader at $prevLog")
        reply(cmd.replyTo)(RaftReply(term, myId, Some(leader), result = false))
      }
    }

    /*
     * This method should be called only from the statehandler
     * to ensure that a state does not become active during replay and becomes active
     * only during execution of protocol between members.
     */
    def enterMode(
                   timers: TimerScheduler[RaftCmd],
                   context: ActorContext[RaftCmd],
                   clusterConfig: Cluster
                 ): Unit = {
      startHeartbeatTimer(timers, context)
    }
  }

  /** base class for all commands exchanged internally between raft servers */
  sealed trait RaftCmd extends Any

  sealed trait RaftCmdWithTerm extends RaftCmd with Term

  sealed trait ExpectingReply[Reply] {
    def replyTo: ActorRef[Reply]
  }

  sealed trait RaftCmdWithTermExpectingReply
    extends RaftCmdWithTerm
      with ExpectingReply[RaftReply]

  /* Base class for all commands and replies
   * handled in all modes -
   * for testing (see internal state) as well as
   * for external requests/replies from the client.
   */
  sealed trait TestProto extends RaftCmd

  /**
    * Type safe index value.
    */
  case class Index(idx: Int = 0) extends AnyVal with Ordered[Index] {
    def compare(that: Index): Int = idx compare that.idx

    def +(x: Int): Index = this.copy(idx = idx + x)

    def -(x: Int): Index = this.copy(idx = idx - x)
  }

  /* Unique ID of a server */
  final case class ServerId(id: Int) extends AnyVal

  /**
    * Index of a log entry.
    */
  case class LogIndex(term: Int = 0, index: Index = Index())
    extends Ordered[LogIndex] {

    def compare(that: LogIndex): Int =
      (this.term, this.index) compare(that.term, that.index)

    def +(x: Index): LogIndex = this.copy(index = Index(index.idx + x.idx))

    def prev(): LogIndex = this.copy(index = Index(index.idx - 1))
  }

  /** Reply for GetValue and SetValue commands */
  final case class ValueIs(value: Int) extends ClientReply

  final case class GetValue(replyTo: ActorRef[ClientReply])
    extends ClientCmd
      with ExpectingReply[ClientReply]

  final case class SetValue(value: Int, replyTo: ActorRef[ClientReply])
    extends ClientCmd
      with ExpectingReply[ClientReply]

  final case class SettingValue(value: Int) extends RSMCmd

  /** New election term */
  final case class NewTerm(term: Int, votedFor: Option[ServerId] = None)
    extends Event

  /** A Vote was received */
  final case class GotVote(term: Int, voter: ServerId) extends Event

  /**
    * Each log entry has a sequence number (index) and command
    * associated with it.
    */
  case class Log(term: Int, cmd: RSMCmd) extends Event

  /** Entries in logs that conflict (same index but different term) with leader */
  case class ConflictingEntries(
                                 idx: Index,
                                 term: Int,
                                 votedFor: Option[ServerId]
                               ) extends Event

  /**
    * Logs - sequence of commands to be applied in order.
    */
  case class Logs(
                   // Needed because of snapshots
                   startingIndex: Index = Index(),
                   logs: Array[Log] = Array(),
                   committed: Index = Index(),
                   applied: Index = Index()
                 ) {
    def lastLogIndex(): LogIndex =
      LogIndex(term = logs.lastOption.map(_.term).getOrElse(0), index = startingIndex + logs.length)
  }

  /**
    * Follower
    */
  final case class Follower(
                             override val myId: ServerId,
                             override val currentTerm: Int = 0,
                             votedFor: Option[ServerId] = None,
                             override val rsm: Logs = Logs()
  ) extends State {

    def commandhandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ): CommandHandler[RaftCmd, Event, State] = CommandHandler.command {
      case cmd@AppendEntries(term, leader, _, prevLog, _, logs)
        if term == currentTerm =>
        if (prevLog.index.idx == 0 && logs.length == 0) {
          context.log.info(s"Got heartbeat from leader: $leader for $term")
        }
        context.log.info(s"Got logs from leader $leader in $term")
        processLogs(timers, context, clusterConfig, cmd)
      case HeartbeatTimeout =>
        Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case cmd@RequestVote(term, candidate, lastLog, _)
        if votedFor.isEmpty && lastLog >= rsm.lastLogIndex() =>
        context.log.info(s"Voting for $candidate in $term")
        Effect
          .persist(NewTerm(term, Some(candidate)))
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, Some(candidate), result = true)
          }
      case cmd@RequestVote(term, _, _, _) =>
        context.log.info(s"Rejecting vote in $term - ${rsm.logs.lastOption} - ${rsm.lastLogIndex()}")
        Effect.none
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, votedFor, result = false)
          }
      case _ => Effect.unhandled
    }

    override def getMode: String = "Follower"
  }

  /** As a candidate solicit and vote in election.
    * If votes from quorum become a leader or follow a leader */
  final case class Candidate(
      override val myId: ServerId,
      override val currentTerm: Int = 0,
      votedFor: Option[ServerId] = None,
      votes: Set[ServerId] = Set(),
      override val rsm: Logs = Logs()
  ) extends State {

    override def enterMode(
                            timers: TimerScheduler[RaftCmd],
                            context: ActorContext[RaftCmd],
                            clusterConfig: Cluster
                          ): Unit = {
      context.log.info(s"Requesting votes in term $currentTerm")
      clusterConfig.memberRefs
        .filter(member => !votes.contains(member._1))
        .foreach { member =>
          context.log.info(
            s"Requesting vote in term $currentTerm from: $member._1"
          )
          member._2 ! RequestVote(
            currentTerm,
            myId,
            rsm.lastLogIndex(),
            context.self
          )
        }
      super.enterMode(timers, context, clusterConfig)
    }

    override def eventHandler(clusterConfig: Cluster, evt: Event): State =
      evt match {
        case GotVote(term, voter)
          if term == currentTerm && clusterConfig.members.contains(voter) =>
          val voters = votes + voter
          if (voters.size >= clusterConfig.quorumSize) {
            Leader(myId, currentTerm, value, rsm, clusterConfig.clusterSize)
          } else {
            copy(votes = voters)
          }
        case _ => super.eventHandler(clusterConfig, evt)
      }

    override def commandhandler(
                                 timers: TimerScheduler[RaftCmd],
                                 context: ActorContext[RaftCmd],
                                 clusterConfig: Cluster
                               ): CommandHandler[RaftCmd, Event, State] = CommandHandler.command {
      case cmd@AppendEntries(term, leader, _, _, _, _) =>
        /* Transition to follower. What if term is same currentTerm? and
         * the leader crashes? In that case, entire existing quorum set of
         * servers won't be transitioning to candidate themselves and they
         * won't vote for us either or anyone else. In that case, election
         * will timeout and goto next term.
         */
        context.log.info(s"Got heartbeat from leader: $leader for $term")
        Effect
          .persist(NewTerm(term, None))
          .thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, votedFor, result = true)
          }
      case cmd@RequestVote(term, candidate, lastLog, _)
        if term > currentTerm && lastLog >= rsm.lastLogIndex() =>
        context.log.info(
          s"Got request vote with higher term - $term than $currentTerm"
        )
        Effect
          .persist(NewTerm(term, Some(candidate)))
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            RaftReply(term, myId, Some(candidate), result = true)
          }
      case cmd@RequestVote(term, _, _, _) =>
        Effect.none
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd.replyTo) { _ =>
            context.log.info(
              s"Rejecting vote in the same term - $term ($currentTerm) as a candidate"
            )
            RaftReply(term, myId, votedFor, result = false)
          }
      case RaftReply(term, voter, votedFor, result)
        if term == currentTerm && votedFor.contains(myId) && result =>
        context.log.info(s"Got vote from $voter")
        Effect.persist(GotVote(term, voter)).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case HeartbeatTimeout =>
        context.log.info(s"Initiating new election with $currentTerm + 1")
        Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case _ => Effect.none
    }

    override def getMode: String = "Candidate"
  }

  /** As a leader, accept commands from the user, and commit after getting
    * ack from the quorum */
  final case class Leader(
      override val myId: ServerId,
      override val currentTerm: Int,
      override val value: Int = 0,
      override val rsm: Logs = Logs(),
      clusterSize: Int
  ) extends State {

    override def enterMode(
                            timers: TimerScheduler[RaftCmd],
                            context: ActorContext[RaftCmd],
                            clusterConfig: Cluster
                          ): Unit = {
      super.enterMode(timers, context, clusterConfig)
      sendHeartBeat(timers, context, clusterConfig)
      if (rsm.lastLogIndex().term != currentTerm) {
        context.log.info(s"$myId has become leader")
        // Generate no-op event to trigger log replication
        context.self ! NoOp()
      }
    }

    def sendHeartBeat(
                       timers: TimerScheduler[RaftCmd],
                       context: ActorContext[RaftCmd],
                       clusterConfig: Cluster
                     ): Unit = {
      clusterConfig.memberRefs.foreach { member =>
        member._2 ! AppendEntries(
          currentTerm,
          myId,
          context.self,
          LogIndex(),
          rsm.committed,
          Array[Log]()
        )
      }
    }

    override def commandhandler(
                                 timers: TimerScheduler[RaftCmd],
                                 context: ActorContext[RaftCmd],
                                 clusterConfig: Cluster
                               ): CommandHandler[RaftCmd, Event, State] = CommandHandler.command {
      case HeartbeatTimeout =>
        Effect.none.thenRun { _: Raft.State =>
          sendHeartBeat(timers, context, clusterConfig)
        }

      case NoOp() =>
        Effect
          .persist(
            Log(
              term = currentTerm,
              cmd = NoOpCmd()
            )
          )
          .thenRun {
            case st: Leader =>
              val replicatorActr: ActorRef[Replicator.ReplicatorMsg] =
                context.spawn(
                  Replicator(
                    currentTerm,
                    context.self,
                    clusterConfig,
                    st.rsm
                  ),
                  replicatorName
                )
            case _ => context.log.error("Invalid state transition")
          }

      case cmd: GetValue =>
        Effect.none.thenReply(cmd.replyTo) { s: State =>
          ValueIs(s.value)
        }

      case SetValue(v, replyTo) =>
        Effect
          .persist(
            Log(
              term = currentTerm,
              cmd = SettingValue(v)
            )
          )
          .thenRun { st: State =>
            context.log.info(
              s"Starting quorum for setting value from $replyTo: $v"
            )
            st match {
              case s: Leader =>
                s.sendAppendEntries(timers, context, clusterConfig, replyTo)
              case _ =>
                assert(
                  assertion = false,
                  "After setting value, raft has to be a leader"
                )
            }
          }
      case cmd@ClientReplicated(index, _) =>
        context.log.info(s"Committed value - $index")
        Effect.none
          .thenReply(cmd.replyTo) { s: State =>
            context.log.debug(
              s"Replying back to the ${cmd.replyTo}: ${s.value}"
            )
            /* TODO: We need to calculate value only after applying
             * log entries after committed
             */
            ValueIs(s.value)
          }
      case _ => Effect.none
    }

    def sendAppendEntries(
                           timers: TimerScheduler[RaftCmd],
                           context: ActorContext[RaftCmd],
                           clusterConfig: Cluster,
                           replyTo: ActorRef[ClientReply]
                         ): Unit = {

      val replicatorActr: ActorRef[Replicator.ReplicatorMsg] = context
        .child(replicatorName)
        .map(_.unsafeUpcast[Replicator.ReplicatorMsg])
        .getOrElse({
          context.spawn(
            Replicator(currentTerm, context.self, clusterConfig, rsm),
            replicatorName
          )
        })
      // Send the last log for replication
      replicatorActr ! LogAppended(rsm.logs.last, replyTo)
    }

    def replicatorName = s"""leader${myId.id}term$currentTerm"""

    override def eventHandler(clusterConfig: Cluster, evt: Event): State =
    // TODO - During replay we need to handle Log events in candidate state
      evt match {
        case cmd@Log(_, SettingValue(v)) =>
          val newLogs = rsm.logs :+ cmd
          this.copy(value = v, rsm = rsm.copy(logs = newLogs))
        case cmd@Log(_, NoOpCmd()) =>
          val newLogs = rsm.logs :+ cmd
          this.copy(rsm = rsm.copy(logs = newLogs))
        case _ => super.eventHandler(clusterConfig, evt)
      }

    override def getMode: String = "Leader"
  }

  /** Message indicating a entry at the index should be committed */
  final case class Committed(index: Index) extends RaftCmd

  final case class ClientReplicated(
      index: Index,
      replyTo: ActorRef[ClientReply]
  ) extends RaftCmd
      with ExpectingReply[ClientReply]

  /** Reply sent in response to all raft commands */
  final case class RaftReply(
      term: Int,
      voter: ServerId,
      votedFor: Option[ServerId],
      result: Boolean
  ) extends RaftCmdWithTerm

  /* Request a vote from the members */
  final case class RequestVote(
      term: Int,
      candidate: ServerId,
      lastLog: LogIndex,
      override val replyTo: ActorRef[RaftReply]
  ) extends RaftCmdWithTermExpectingReply

  /* Append entries to the logs */
  final case class AppendEntries(
      term: Int,
      leader: ServerId,
      override val replyTo: ActorRef[RaftReply],
      prevLog: LogIndex,
      leaderCommit: Index,
      log: Array[Log] = Array()
  ) extends RaftCmdWithTermExpectingReply

  /* Used during testing to retrieve current state */
  final case class GetState(replyTo: ActorRef[TestProto])
    extends TestProto
      with ExpectingReply[CurrentState]

  /* Used during testing to retrieve current state */
  final case class CurrentState(id: ServerId, term: Int, mode: String)
    extends TestProto

  // No-op event logged by new leader
  private final case class NoOp() extends ClientCmd

  private[raft] final case class NoOpCmd() extends RSMCmd

  /* heartbeat timeout */
  final case object HeartbeatTimeout extends RaftCmd

}
