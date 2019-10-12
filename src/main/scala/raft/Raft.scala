package raft

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.persistence.typed.{ExpectingReply, PersistenceId, RecoveryCompleted}
import com.typesafe.config.Config

import scala.collection.{mutable, immutable => im}
import scala.concurrent.duration.{Duration, FiniteDuration}
import raft.Replicator.LogAppended

/**
  * We use typed akka and akka persistence to model an Raft server.
  * We persist each new term, votes, and logs.
  */
object Raft {

  /**
    * Type safe index value.
    */
  case class Index(idx: Int = 0) extends AnyVal with Ordered[Index] {
    def compare(that: Index): Int = idx compare that.idx
    def +(x: Int) = this.copy(idx = idx + 1)
    def -(x: Int) = this.copy(idx = idx - 1)
  }

  /* Election Term */
  sealed trait Term {
    def term: Int
  }

  /* Unique ID of a server */
  final case class ServerId(id: Int) extends AnyVal

  /**
    * Index of a log entry.
    */
  case class LogIndex(term: Int = 0, index: Index = Index())
      extends Ordered[LogIndex] {
    import scala.math.Ordered.orderingToOrdered

    def compare(that: LogIndex): Int =
      (this.term, this.index) compare (that.term, that.index)
    def +(x: Index) = this.copy(index = Index(index.idx + x.idx))
    def prev() = this.copy(index = Index(index.idx - 1))
  }

  /**
    * Base class for a external and internal client commands handled by
    * the leader.
    */
  sealed trait ClientCmd extends RaftCmd

  /** Base class for client replies */
  sealed trait ClientReply

  /** Reply for GetValue and SetValue commands */
  final case class ValueIs(value: Int) extends ClientReply

  final case class GetValue(replyTo: ActorRef[ClientReply])
      extends ClientCmd
      with ExpectingReply[ClientReply]

  final case class SetValue(value: Int, replyTo: ActorRef[ClientReply])
      extends ClientCmd
      with ExpectingReply[ClientReply]

  // No-op event logged by new leader
  private final case class NoOp() extends ClientCmd

  /** Base class for all commands logged by the Raft server's RSM */
  sealed trait RSMCmd
  final case class SettingValue(value: Int) extends RSMCmd

  private[raft] final case class NoOpCmd() extends RSMCmd

  /** Base class for all events that cause state transition.
    * Also all events are persisted in a log for recovery
    */
  sealed trait Event

  /** New election term */
  final case class NewTerm(term: Int, votedFor: Option[ServerId] = None)
      extends Event

  /** A Vote was received */
  final case class GotVote(term: Int, voter: ServerId) extends Event

  /**
    * Each log entry has a sequence number (index) and command
    * associated with it.
    */
  case class Log(index: LogIndex, cmd: RSMCmd) extends Event

  /** Entries in logs that conflict (same index but different term) with leader */
  case class ConflictingEntries(
      idx: Index,
      term: Int,
      votedFor: Option[ServerId]
  ) extends Event

  /**
    * Logs - sequence of commands applied to be applied in order.
    */
  case class Logs(
      logs: Array[Log] = Array(),
      committed: Index = Index(),
      applied: LogIndex = LogIndex()
  ) {
    def lastLogIndex(): LogIndex = {
      if (logs.isEmpty) {
        LogIndex()
      } else {
        logs.last.index
      }
    }
  }

  /** The persistent state stored by all servers. */
  sealed trait State {

    /* The current value. It can be set by SetCmd and re-trived by GetVal */
    def value: Int = 0

    def eventHandler(clusterConfig: Cluster, evt: Event): State = evt match {
      /*
       * When votedFor == empty or votedFor != myId became follower
       * Else goto Candidate.
       */
      case NewTerm(term, votedFor) if votedFor == Some(myId) =>
        Candidate(myId, term, votedFor, rsm = rsm)
      case NewTerm(term, votedFor) => Follower(myId, term, votedFor, rsm = rsm)
      // TODO - This should be moved to the Follower state
      case ConflictingEntries(Index(idx), term, votedFor) =>
        Follower(
          myId,
          term,
          votedFor,
          rsm.copy(logs = rsm.logs.drop(rsm.logs.length - idx))
        )
    }

    /* The unique of the server */
    def myId: ServerId = ServerId(0)

    /* The current term of this server */
    def currentTerm: Int = 0

    /* Txn logs for this server */
    def rsm: Logs = Logs()

    /* Whether leader or follower */
    def getMode: String

    /*
     * This method should be called only from the statehandler
     * to ensure that a state does not become active during replay and becomes active
     * only during executive of protocol between members.
     */
    def enterMode(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ) = {
      startHeartbeatTimer(timers, context)
    }

    /**
      * Handles given command in the context of the current state.
      */
    def commandhandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ): CommandHandler[RaftCmd, Raft.Event, Raft.State]

    /**
      * The default handler for all server types (modes).
      */
    def defaultHandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ): PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
      case cmd @ RequestVote(term, candidate, _, _) if term > currentTerm =>
        context.log.info(
          s"Voting for $candidate in $term and transitioning to follower"
        )
        Effect
          .persist(NewTerm(term, Some(candidate)))
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd) { _ =>
            RaftReply(term, myId, Some(candidate), true)
          }
      case cmd @ AppendEntries(term, leader, _, prevLog, leaderCommit, logs)
          if term > currentTerm =>
        processLogs(timers, context, clusterConfig, cmd)
      case cmd: RaftCmdWithTermExpectingReply if cmd.term < currentTerm =>
        // Reject all cmds with term less than current term
        Effect.reply(cmd)(RaftReply(currentTerm, myId, None, false))
      case e: Term if e.term > currentTerm =>
        Effect.persist(NewTerm(e.term)).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case e: Term if e.term < currentTerm =>
        Effect.none // Ignore stale messages
      case c: GetState =>
        Effect.reply(c)(CurrentState(myId, currentTerm, getMode))
    }

    def processLogs(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster,
        cmd: AppendEntries
    ) = {
      val AppendEntries(term, leader, _, prevLog, leaderCommit, logs) = cmd
      context.log.info(s"Got heartbeat from new leader $leader in $term")
      val prevLogIdx =
        rsm.logs.indexWhere(x => x.index.index == prevLog.index)

      if (prevLogIdx == -1) {
        if (prevLog.index.idx == 0) { // Logs start from index = 1
          // Accept all logs
          context.log.debug(s"Initiating logs from $leader in $term")
          val evts =
            if (logs.isEmpty) Seq(NewTerm(term, Some(leader))) else logs.to(Seq)
          Effect
            .persist(evts) // Persists log entries sent by the leader
            .thenRun { state: State =>
              state.enterMode(timers, context, clusterConfig)
            }
            .thenReply(cmd) { _ =>
              RaftReply(term, myId, None, true)
            }
        } else {
          // Reject entries
          context.log.info("Rejecting log entries from $leader at $prevLog")
          Effect
            .persist(NewTerm(term, Some(leader)))
            .thenRun { state: State =>
              state.enterMode(timers, context, clusterConfig)
            }
            .thenReply(cmd) { _ =>
              RaftReply(term, myId, Some(leader), false)
            }
        }
      } else {
        // Check the term at prevLog.index
        if (rsm.logs(prevLogIdx).index.term != prevLog.term) {
          // Found matching entry to prevLog with the same index and but conflicting term
          // Reject the AppendEntries request and become a follower.
          context.log.info("Rejecting log entries from $leader at $prevLog")
          Effect
            .persist(NewTerm(term, Some(leader)))
            .thenRun { state: State =>
              state.enterMode(timers, context, clusterConfig)
            }
            .thenReply(cmd) { _ =>
              RaftReply(term, myId, Some(leader), false)
            }
        } else {
          // Accept new logs
          var idx = prevLogIdx
          var newidx = 0
          var conflict = false
          while (idx < rsm.logs.length && newidx < logs.length && !conflict) {
            assert(
              rsm.logs(idx).index.index == logs(newidx).index.index,
              "logs should be in order"
            )
            if (rsm.logs(idx).index.term == logs(newidx).index.term) {
              // Skip ahead
              idx += 1
              newidx += 1
            } else {
              // Conflicting entries, delete all logs from this point onwards.
              conflict = true
            }
          }
          if (idx < rsm.logs.length) {
            // We need to remove all entries from idx onwards.
            val evts: im.Seq[Event] = ConflictingEntries(
              Index(idx),
              term,
              Some(leader)
            ) +: logs.drop(logs.length - newidx).to(Seq)
            Effect
              .persist(evts)
              .thenRun { state: State =>
                state.enterMode(timers, context, clusterConfig)
              }
              .thenReply(cmd) { _ =>
                RaftReply(term, myId, Some(leader), true)
              }
          } else {
            val evts: im.Seq[Event] = NewTerm(term, Some(leader)) +: logs
              .drop(logs.length - newidx)
              .to(Seq)
            Effect
              .persist(evts)
              .thenRun { state: State =>
                state.enterMode(timers, context, clusterConfig)
              }
              .thenReply(cmd) { _ =>
                RaftReply(term, myId, Some(leader), true)
              }
          }
        }
      }
    }
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
      case cmd =>
        cmd match {
          case cmd @ AppendEntries(term, leader, _, prevLog, leaderCommit, logs)
              if term == currentTerm =>
            if (prevLog.index.idx == 0 && logs.length == 0) {
              context.log.info(s"Got heartbeat from leader: $leader for $term")
            }
            context.log.info(s"Got logs from leader $leader in $term")
            processLogs(timers, context, clusterConfig, cmd)
          case HeartbeatTimeout =>
            Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun {
              state =>
                state.enterMode(timers, context, clusterConfig)
            }
          case cmd @ RequestVote(term, candidate, lastLog, _)
              if votedFor.isEmpty && lastLog >= rsm.lastLogIndex() =>
            context.log.info(s"Voting for $candidate in $term")
            Effect
              .persist(NewTerm(term, Some(candidate)))
              .thenRun { state: Raft.State =>
                state.enterMode(timers, context, clusterConfig)
              }
              .thenReply(cmd) { _ =>
                RaftReply(term, myId, Some(candidate), true)
              }
          case cmd @ RequestVote(term, _, _, _) =>
            Effect.none
              .thenRun { state: Raft.State =>
                state.enterMode(timers, context, clusterConfig)
              }
              .thenReply(cmd) { _ =>
                RaftReply(term, myId, votedFor, false)
              }
          case _ => Effect.unhandled
        }
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
    ) = {
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

    override def eventHandler(clusterConfig: Cluster, evt: Event): State = {
      evt match {
        case GotVote(term, voter)
            if term == currentTerm && clusterConfig.members.contains(voter) => {
          val voters = votes + voter
          if (voters.size >= clusterConfig.quorumSize) {
            Leader(myId, currentTerm, value, rsm, clusterConfig.clusterSize)
          } else {
            copy(votes = voters)
          }
        }
        case _ => super.eventHandler(clusterConfig, evt)
      }
    }

    override def commandhandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ) = CommandHandler.command { cmd =>
      cmd match {
        case cmd @ AppendEntries(term, leader, _, _, _, _) =>
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
            .thenReply(cmd) { _ =>
              RaftReply(term, myId, votedFor, true)
            }
        case cmd @ RequestVote(term, candidate, lastLog, _)
            if term > currentTerm && lastLog >= rsm.lastLogIndex() =>
          context.log.info(
            s"Got request vote with higher term - $term than $currentTerm"
          )
          Effect
            .persist(NewTerm(term, Some(candidate)))
            .thenRun { state: Raft.State =>
              state.enterMode(timers, context, clusterConfig)
            }
            .thenReply(cmd) { _ =>
              RaftReply(term, myId, Some(candidate), true)
            }
        case cmd @ RequestVote(term, _, _, _) =>
          Effect.none
            .thenRun { state: Raft.State =>
              state.enterMode(timers, context, clusterConfig)
            }
            .thenReply(cmd) { _ =>
              context.log.info(
                s"Rejecting vote in the same term - $term ($currentTerm) as a candidate"
              )
              RaftReply(term, myId, votedFor, false)
            }
        case RaftReply(term, voter, votedFor, result)
            if term == currentTerm && votedFor == Some(myId) && result =>
          context.log.info(s"Got vote from $voter")
          Effect.persist(GotVote(term, voter)).thenRun { state =>
            state.enterMode(timers, context, clusterConfig)
          }
        case HeartbeatTimeout =>
          context.log.info(s"Initiating new election with $currentTerm + 1")
          Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun {
            state =>
              state.enterMode(timers, context, clusterConfig)
          }
        case _ => Effect.none
      }
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
    ) = {
      super.enterMode(timers, context, clusterConfig)
      sendHeartBeat(timers, context, clusterConfig)
      if (rsm.lastLogIndex().term != currentTerm) {
        context.log.info(s"${myId} has become leader")
        // Generate no-op event to trigger log replication
        context.self ! NoOp()
      }
    }

    def sendHeartBeat(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ) = {
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

    def replicatorName = s"leader${myId.id}term${currentTerm}"
    def sendAppendEntries(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster,
        replyTo: ActorRef[ClientReply]
    ) = {

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

    override def commandhandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ) = CommandHandler.command { cmd =>
      cmd match {
        case HeartbeatTimeout =>
          Effect.none.thenRun { state: Raft.State =>
            sendHeartBeat(timers, context, clusterConfig)
          }

        case NoOp() =>
          Effect
            .persist(
              Log(
                index = LogIndex(currentTerm, rsm.lastLogIndex().index + 1),
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
          Effect.none.thenReply(cmd) { s: State =>
            ValueIs(s.value)
          }

        case SetValue(v, replyTo) =>
          Effect
            .persist(
              Log(
                index = LogIndex(currentTerm, rsm.lastLogIndex().index + 1),
                cmd = SettingValue(v)
              )
            )
            .thenRun { st: State =>
              context.log.info(
                s"Starting quorum for setting value from ${replyTo}: ${v}"
              )
              st match {
                case s: Leader =>
                  s.sendAppendEntries(timers, context, clusterConfig, replyTo)
                case _ =>
                  assert(false, "After setting value, raft has to be a leader")
              }
            }
        case cmd @ ClientReplicated(index, _) =>
          context.log.info(s"Committed value - $index")
          Effect.none
            .thenReply(cmd) { s: State =>
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
    }

    override def eventHandler(clusterConfig: Cluster, evt: Event): State =
      // TODO - During replay we need to handle Log events in candidate state
      evt match {
        case cmd @ Log(idx, SettingValue(v)) =>
          val newLogs = rsm.logs :+ cmd
          this.copy(value = v, rsm = rsm.copy(logs = newLogs))
        case cmd @ Log(idx, NoOpCmd()) =>
          val newLogs = rsm.logs :+ cmd
          this.copy(rsm = rsm.copy(logs = newLogs))
        case _ => super.eventHandler(clusterConfig, evt)
      }

    override def getMode: String = "Leader"
  }

  /** base class for all commands exchanged internally between raft servers */
  sealed trait RaftCmd

  sealed trait RaftCmdWithTerm extends RaftCmd with Term

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

  sealed trait RaftCmdWithTermExpectingReply
      extends RaftCmdWithTerm
      with ExpectingReply[RaftReply]

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

  /* heartbeat timeout */
  final case object HeartbeatTimeout extends RaftCmd

  /* Base class for all commands and replies
   * handled in all modes -
   * for testing (see internal state) as well as
   * for external requests/replies from the client.
   */
  sealed trait TestProto extends RaftCmd

  /* Used during testing to retrieve current state */
  final case class GetState(replyTo: ActorRef[TestProto])
      extends TestProto
      with ExpectingReply[CurrentState]

  /* Used during testing to retrieve current state */
  final case class CurrentState(id: ServerId, term: Int, mode: String)
      extends TestProto

  /** Starts the heartbeat/election timer */
  def startHeartbeatTimer(
      timers: TimerScheduler[RaftCmd],
      context: ActorContext[RaftCmd]
  ) = {
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration =
      Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

    timers.startSingleTimer(HeartbeatTimeout, HeartbeatTimeout, electionTimeout)
  }

  def apply(id: Option[ServerId] = None)(
      implicit clusterConfig: Cluster
  ): Behavior[RaftCmd] = Behaviors.setup { context: ActorContext[RaftCmd] =>
    val myId: ServerId = id.getOrElse(clusterConfig.myId)
    val persistenceId = PersistenceId(s"raft-server-typed-${myId}.id")

    Behaviors.withTimers { timers: TimerScheduler[RaftCmd] =>
      EventSourcedBehavior[RaftCmd, Event, State](
        persistenceId = persistenceId,
        emptyState = Follower(myId),
        commandHandler = dispatcher(timers, context, clusterConfig),
        eventHandler = eventHandler(clusterConfig)
      ).snapshotWhen {
          case (state, ConflictingEntries(_, _, _), _) => true
          case _                                       => false
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
    (state: Raft.State, cmd: RaftCmd) =>
      {
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

}
