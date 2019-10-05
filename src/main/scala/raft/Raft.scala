package raft

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.persistence.typed.{ExpectingReply, PersistenceId, RecoveryCompleted}
import com.typesafe.config.Config

import scala.collection.{mutable, immutable => im}
import scala.concurrent.duration.{Duration, FiniteDuration}

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

  /** Base class for all commands logged by the Raft server's RSM */
  sealed trait RSMCmd
  final case class SettingValue(value: Int) extends RSMCmd

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

  /**
    * Logs - sequence of commands applied to be applied in order.
    */
  case class Logs(
      logs: Array[Log] = Array(),
      committed: LogIndex = LogIndex(),
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
      * The default handler for all states.
      */
    def defaultHandler(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ): PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
      case cmd @ RequestVote(term, candidate, _, _) if term > currentTerm =>
        context.log.info(s"Voting for $candidate in $term")
        Effect
          .persist(NewTerm(term, Some(candidate)))
          .thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd) { _ =>
            RaftReply(term, myId, Some(candidate), true)
          }
      case cmd @ AppendEntries(term, leader, _, _, _, logs)
          if term > currentTerm =>
        context.log.info(s"Got heartbeat from new leader $leader in $term")
        /* We assume that leader is sending us only the diff/new logs for the RSM and not
         * internal leader election events.
         */
        val evt: Event = NewTerm(term, None)
        val newLogs: im.Seq[Event] = logs.iterator.map(identity).to(Seq) ++ im
          .Seq(evt)
        Effect
          .persist(newLogs)
          .thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
          }
          .thenReply(cmd) { _ =>
            RaftReply(term, myId, None, true)
          }
      case cmd: (ExpectingReply[RaftReply] with Term)
          if cmd.term < currentTerm =>
        Effect.reply(cmd)(RaftReply(currentTerm, myId, None, false))
      case e: Term if e.term > currentTerm =>
        Effect.persist(NewTerm(e.term)).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case e: Term if e.term < currentTerm => Effect.none
      case c: GetState =>
        Effect.reply(c)(CurrentState(myId, currentTerm, getMode))
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
          case cmd @ AppendEntries(term, leader, _, _, _, _)
              if term == currentTerm =>
            /* On heartbeat restart the timer, only if heartbeat is for the current term */
            /* TODO: Do we need to verify if heartbeat is from leader of the current term? */
            context.log.info(s"Got heartbeat from leader: $leader for $term")
            Effect.none
              .thenRun { state: State =>
                state.enterMode(timers, context, clusterConfig)
              }
              .thenReply(cmd) { _ =>
                RaftReply(term, myId, votedFor, true)
              }
          case cmd @ AppendEntries(term, leader, _, prevLog, _, logs)
              if term == currentTerm =>
            context.log.info(s"Got logs from leader $leader in $term")
            /* We assume that leader is sending us only the diff/new logs for the RSM and not
             * internal leader election events.
             */
            val newLogs: im.Seq[Event] = logs.iterator.map(identity).to(Seq)
            val f = rsm.logs.find(_.index.index == prevLog.index)
            if (!f.exists { _.index.index == prevLog.index }) {
              Effect.none.thenReply(cmd) { _ =>
                RaftReply(term, myId, None, false)
              }
            } else if (f.exists(_.index.term != prevLog.term)) {
              Effect.none.thenReply(cmd) { _ =>
                RaftReply(term, myId, None, false)
              }
            } else {
              Effect
                .persist(newLogs)
                .thenRun { state: State =>
                  state.enterMode(timers, context, clusterConfig)
                }
                .thenReply(cmd) { _ =>
                  RaftReply(term, myId, None, true)
                }
            }
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

    val prevIndexToSend: Index = rsm.lastLogIndex().index

    val matchIndex: mutable.Map[ServerId, Index] = mutable.Map()

    val nextIndex: mutable.Map[ServerId, Index] = mutable.Map()

    override def enterMode(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster
    ) = {
      super.enterMode(timers, context, clusterConfig)
      context.log.info("Becoming leader - sending heartbeat")
      sendHeartBeat(timers, context, clusterConfig)
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
      super.enterMode(timers, context, clusterConfig)
    }

    def sendAppendEntries(
        timers: TimerScheduler[RaftCmd],
        context: ActorContext[RaftCmd],
        clusterConfig: Cluster,
        replyTo: ActorRef[ClientReply]
    ) = {
      /*
       * We have two choices on how to send diff to each member:
       * 1. We ask the matchIndex from member and then find the delta
       * 2. We have each memberProxy ask the leader for new diff logs
       * The problem is leader maintains the logs but member/memberproxy maintains the matchIndex.
       * We could just always send lastLog entry
       */
      def replicator(parent: ActorRef[RaftCmd]): Behavior[RaftReply] =
        Behaviors.setup[RaftReply] { _ =>
          def reachedQuroum(): Behavior[RaftReply] = {
            if (matchIndex.values.filter { x: Index =>
                  x >= prevIndexToSend
                }.size >= clusterConfig.quorumSize) {
              Behavior.stopped { () =>
                context.log.info(
                  s"Successful in replication of $prevIndexToSend"
                )
                parent ! Replicated(prevIndexToSend, replyTo)
              }
            } else {
              Behavior.same
            }
          }
          Behaviors.receiveMessage {
            case RaftReply(term, voter, _, result) if term == currentTerm =>
              if (result) {
                matchIndex(voter) = prevIndexToSend
                nextIndex(voter) = prevIndexToSend + 1
                context.log.info(s"Reach quorum on value in $term")
                reachedQuroum()
              } else {
                // TODO: Resend the AppendEntries with lesser index.
                nextIndex(voter) = prevIndexToSend - 1
                Behaviors.same
              }
            case _ => Behaviors.unhandled
          }
        }

      val replicatorRef =
        context.spawn(
          replicator(context.self),
          s"leader${myId.id}-$currentTerm"
        )
      clusterConfig.memberRefs.foreach { member =>
        /* TODO: Get prevLog from each member */
        member._2 ! AppendEntries(
          currentTerm,
          myId,
          replicatorRef,
          rsm.lastLogIndex().prev(),
          rsm.committed,
          rsm.logs.take(1)
        )
      }
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
              }
            }
        case cmd @ Replicated(index, _) =>
          context.log.info(s"Committed value - $index")
          Effect.none
            .thenReply(cmd) { s: State =>
              context.log.debug(
                s"Replying back to the ${cmd.replyTo}: ${s.value}"
              )
              ValueIs(s.value)
            }
        case _ => Effect.none
      }
    }

    override def eventHandler(clusterConfig: Cluster, evt: Event): State =
      evt match {
        case cmd @ Log(idx, SettingValue(v)) =>
          val newLogs = rsm.logs :+ cmd
          this.copy(value = v, rsm = rsm.copy(logs = newLogs, committed = idx))
        case _ => super.eventHandler(clusterConfig, evt)
      }

    override def getMode: String = "Leader"
  }

  sealed trait RaftCmd

  final case class RaftReply(
      term: Int,
      voter: ServerId,
      votedFor: Option[ServerId],
      result: Boolean
  ) extends RaftCmd
      with Term

  /* Request a vote from the members */
  final case class RequestVote[Reply](
      term: Int,
      candidate: ServerId,
      lastLog: LogIndex,
      override val replyTo: ActorRef[RaftReply]
  ) extends RaftCmd
      with Term
      with ExpectingReply[RaftReply]

  /* Append entries to the logs */
  final case class AppendEntries[Reply](
      term: Int,
      leader: ServerId,
      override val replyTo: ActorRef[RaftReply],
      prevLog: LogIndex,
      leaderCommit: LogIndex,
      log: Array[Log] = Array()
  ) extends RaftCmd
      with Term
      with ExpectingReply[RaftReply]

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

  /**
    * Base class for a external and internal client commands handled by
    * the leader.
    */
  sealed trait ClientCmd extends RaftCmd

  sealed trait ClientReply
  final case class ValueIs(value: Int) extends ClientReply

  final case class GetValue(replyTo: ActorRef[ClientReply])
      extends ClientCmd
      with ExpectingReply[ClientReply]

  final case class SetValue(value: Int, replyTo: ActorRef[ClientReply])
      extends ClientCmd
      with ExpectingReply[ClientReply]

  private final case class Replicated(
      index: Index,
      replyTo: ActorRef[ClientReply]
  ) extends RaftCmd
      with ExpectingReply[ClientReply]

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
      ).receiveSignal {
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
