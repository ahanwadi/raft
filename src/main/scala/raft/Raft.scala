package raft

import scala.collection.{ immutable => im }
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.persistence.typed.{ExpectingReply, PersistenceId, RecoveryCompleted}
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

object Raft {

  sealed trait ServerId {
    def myId: Int
  }


  case class LogIndex(term: Int =0, index: Int = 0)
  case class Log(index: LogIndex, cmd: RSMCmd) extends Event

  case class RSM(logs: Array[Log] = Array(), committed: LogIndex = LogIndex(), applied: LogIndex = LogIndex()) {
    def getPrevIndex() = {
      if (logs.isEmpty) {
        LogIndex()
      } else {
        logs.last.index
      }
    }
  }

  /* Current Election Term */
  sealed trait Term {
    def term: Int
  }

  type Index = Int


  // The persistent state stored by all servers.
  sealed trait State {
    def value: Int = 0

    def eventHandler(clusterConfig: Cluster, evt: Event): State = evt match {
      /*
       * When votedFor == empty or votedFor != myId became follower
       * Else goto Candidate.
       */
      case NewTerm(term, votedFor) if votedFor == Some(myId) => Candidate(myId, term, votedFor, rsm = rsm)
      case NewTerm(term, votedFor) => Follower(myId, term, votedFor, rsm = rsm)
    }

    def myId: Int = 0
    def currentTerm: Int = 0

    def rsm: RSM = RSM()
    def getMode: String

    def enterMode(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = {
      startHeartbeatTimer(timers, context)
    }

    def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster): CommandHandler[RaftCmd, Raft.Event, Raft.State]

    def defhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) : PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
      case cmd @ RequestVote(term, candidate, _) if term > currentTerm =>
        context.log.info(s"Voting for $candidate in $term")
        Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
          state.enterMode(timers, context, clusterConfig)
        }.thenReply(cmd) { _ =>
          RaftReply(term, myId, Some(candidate), true)
        }
      case cmd @ AppendEntries(term, leader, _, _, _, logs) if term > currentTerm =>
        context.log.info(s"Got heartbeat from new leader $leader in $term")
        /* We assume that leader is sending us only the diff/new logs for the RSM and not
         * internal leader election events.
         */
        val evt: Event = NewTerm(term, None)
        val newLogs: im.Seq[Event] = logs.map(identity)(collection.breakOut) ++ im.Seq(evt)
        Effect.persist(newLogs).thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
        }.thenReply(cmd) { _ =>
          RaftReply(term, myId, None, true)
        }
      case cmd: (ExpectingReply[RaftReply] with Term) if cmd.term < currentTerm =>
        Effect.reply(cmd)(RaftReply(currentTerm, myId, None, false))
      case e: Term if e.term > currentTerm =>
        Effect.persist(NewTerm(e.term)).thenRun { state =>
          state.enterMode(timers, context, clusterConfig)
        }
      case e: Term if e.term < currentTerm => Effect.none
      case c: GetState =>
        Effect.reply(c) (CurrentState(myId, currentTerm, getMode))
    }
  }

  final case class Follower(override val myId: Int, override val currentTerm: Int = 0, votedFor: Option[Int] = None,
                            override val rsm: RSM = RSM()) extends State {

    def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster): CommandHandler[RaftCmd, Event, State] = CommandHandler.command { case cmd =>

      cmd match {
        case cmd @ AppendEntries(term, leader, _, _, _, _) if term == currentTerm =>
          /* On heartbeat restart the timer, only if heartbeat is for the current term */
          /* TODO: Do we need to verify if heartbeat is from leader of the current term? */
          context.log.info(s"Got heartbeat from leader: $leader for $term")
          Effect.none.thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            RaftReply(term, myId, votedFor, true)
          }
        case HeartbeatTimeout =>
          Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
            state.enterMode(timers, context, clusterConfig)
          }
        case cmd @ RequestVote(term, candidate, _) if votedFor.isEmpty =>
          context.log.info(s"Voting for $candidate in $term")
          Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            RaftReply(term, myId, Some(candidate), true)
          }
        case cmd @ RequestVote(term, _, _) =>
          Effect.none.thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            RaftReply(term, myId, votedFor, false)
          }
        case _ => Effect.unhandled
      }
    }

    override def getMode: String = "Follower"
  }

  final case class Candidate(override val myId: Int, override val currentTerm: Int = 0, votedFor: Option[Int] = None,
                             votes: Set[Int] = Set(),
                             override val rsm: RSM = RSM()) extends State {

    override def enterMode(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = {
      context.log.info(s"Requesting votes in term $currentTerm")
      clusterConfig.memberRefs.filter(member => !votes.contains(member._1)).foreach { member =>
        context.log.info(s"Requesting vote in term $currentTerm from: $member._1")
        member._2 ! RequestVote(currentTerm, myId, context.self)
      }
      super.enterMode(timers, context, clusterConfig)
    }

    override def eventHandler(clusterConfig: Cluster, evt: Event): State = {
      evt match {
        case GotVote(term, voter) if term == currentTerm && clusterConfig.members.contains(voter) => {
          val voters = votes + voter
          if (voters.size >= clusterConfig.quorumSize) {
            Leader(myId, currentTerm, value, rsm)
          } else {
            copy(votes = voters)
          }
        }
        case _ => super.eventHandler(clusterConfig, evt)
      }
    }

    override def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = CommandHandler.command { cmd =>
      cmd match {
        case cmd @ AppendEntries(term, leader, _, _, _, _) =>
          /* Transition to follower. What if term is same currentTerm? and
           * the leader crashes? In that case, entire existing quorum set of
           * servers won't be transitioning to candidate themselves and they
           * won't vote for us either or anyone else. In that case, election
           * will timeout and goto next term.
           */
          context.log.info(s"Got heartbeat from leader: $leader for $term")
          Effect.persist(NewTerm(term, None)).thenRun { state: State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            RaftReply(term, myId, votedFor, true)
          }
        case cmd @ RequestVote(term, candidate, _) if term > currentTerm =>
          context.log.info(s"Got request vote with higher term - $term than $currentTerm")
          Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            RaftReply(term, myId, Some(candidate), true)
          }
        case cmd @ RequestVote(term, _, _) =>
          Effect.none.thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }.thenReply(cmd) { _ =>
            context.log.info(s"Rejecting vote in the same term - $term ($currentTerm) as a candidate")
            RaftReply(term, myId, votedFor, false)
          }
        case RaftReply(term, voter, votedFor, result) if term == currentTerm && votedFor == Some(myId) && result =>
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
    }

    override def getMode: String = "Candidate"
  }

  final case class Leader(override val myId: Int, override val currentTerm: Int,
                          override val value: Int = 0, override val rsm: RSM = RSM()) extends State {

    def getAppendEntries(member: Int, context: ActorContext[RaftCmd]): AppendEntries[RaftCmd] = {
      AppendEntries(currentTerm, myId, context.self, rsm.getPrevIndex(), rsm.committed, rsm.logs.take(1))
    }

    override def enterMode(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = {
      clusterConfig.memberRefs.foreach { member =>
        /* TODO: Get prevLog from each member */
        member._2 ! AppendEntries(currentTerm, myId, context.self, rsm.getPrevIndex(), rsm.committed)
      }
      super.enterMode(timers, context, clusterConfig)
    }

    def sendAppendEntries(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = {
      clusterConfig.memberRefs.foreach { member =>
        /* TODO: Get prevLog from each member */
        member._2 ! AppendEntries(currentTerm, myId, context.self, rsm.getPrevIndex(), rsm.committed, rsm.logs.take(1))
      }
    }

    override def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) = CommandHandler.command { cmd =>
      cmd match {
        case HeartbeatTimeout =>
          Effect.none.thenRun { state: Raft.State =>
            state.enterMode(timers, context, clusterConfig)
          }
        case cmd : GetValue =>
          Effect.none.thenReply(cmd) { s: State =>
           ValueIs(s.value)
          }
        case cmd @ SetValue(v, _) =>
          Effect.persist(Log(index = LogIndex(currentTerm, rsm.committed.index+1), cmd = SettingValue(v)))
            .thenRun { st: State =>
              st match {
                case s: Leader =>
                  s.sendAppendEntries(timers, context, clusterConfig)
                  Effect.none
                case _ => Effect.none
              }
            }
            .thenReply(cmd) { s: State =>
              ValueIs(s.value)
            }
        case _ => Effect.none
      }
    }

    override def eventHandler(clusterConfig: Cluster, evt: Event): State = evt match {
      case cmd @ Log(index, SettingValue(v)) =>
        val newCommitIndex = rsm.committed.copy(term = currentTerm, index = rsm.committed.index + 1)
        val newLogs = rsm.logs :+ cmd
        this.copy(value = v, rsm = rsm.copy(committed = index, logs = newLogs))
      case _ => super.eventHandler(clusterConfig, evt)
    }

    override def getMode: String = "Leader"
  }

  sealed trait Event {
  }

  final case class NewTerm(term: Int, votedFor: Option[Int] = None) extends Event
  final case class GotVote(term: Int, voters: Int) extends Event

  sealed trait RSMCmd
  final case class SettingValue(value: Int) extends RSMCmd


  sealed trait RaftCmd
  final case class RaftReply(term: Int, voter: Int, votedFor: Option[Int], result: Boolean) extends RaftCmd with Term
  final case class RequestVote[Reply](term: Int, candidate: Int, override val replyTo: ActorRef[RaftCmd]) extends RaftCmd with Term with ExpectingReply[RaftReply]
  final case class AppendEntries[Reply](term: Int, leader: Int, override val replyTo: ActorRef[RaftCmd],
                                        prevLog: LogIndex, leaderCommit: LogIndex, log: Array[Log] = Array()) extends RaftCmd with Term with ExpectingReply[RaftReply]
  final case object HeartbeatTimeout extends RaftCmd

  sealed trait ClientProto extends RaftCmd
  final case class GetState(replyTo: ActorRef[ClientProto]) extends ClientProto with ExpectingReply[CurrentState]
  final case class CurrentState(id: Int, term: Int, mode: String) extends ClientProto

  sealed trait ClientCmd extends ClientProto
  final case class GetValue(replyTo: ActorRef[ClientCmd]) extends ClientCmd with ExpectingReply[ClientCmd]
  final case class SetValue(value: Int, replyTo: ActorRef[ClientCmd]) extends ClientCmd with ExpectingReply[ClientCmd]
  final case class ValueIs(value: Int) extends ClientCmd

  def startHeartbeatTimer(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = {
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

    timers.startSingleTimer(HeartbeatTimeout, HeartbeatTimeout, electionTimeout)
  }

  def apply(id: Option[Int] = None)(implicit clusterConfig: Cluster): Behavior[RaftCmd] = Behaviors.setup { context: ActorContext[RaftCmd] =>
//    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val myId: Int = id.getOrElse(clusterConfig.myId) // raftConfig.getInt("myId"))
    val persistenceId = PersistenceId(s"raft-server-typed-$myId")

    Behaviors.withTimers { timers: TimerScheduler[RaftCmd] =>
      EventSourcedBehavior[RaftCmd, Event, State](
        persistenceId = persistenceId,
        emptyState = Follower(myId),
        commandHandler = dispatcher(timers, context, clusterConfig),
        eventHandler = eventHandler(clusterConfig)
      ).receiveSignal {
          case (state, RecoveryCompleted) => state.enterMode(timers, context, clusterConfig)
        }
    }
  }

  def dispatcher(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd], clusterConfig: Cluster) : (Raft.State, RaftCmd) => Effect[Raft.Event, Raft.State] = {
    (state: Raft.State, cmd: RaftCmd) => {
      def secondhandler: PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = { case c =>
        state.commandhandler(timers, context, clusterConfig)(state, c)
      }
      val y: Effect[Event, State] =
        state.defhandler(timers, context = context, clusterConfig).orElse(secondhandler)(cmd)
      y
    }
  }


  def eventHandler(clusterConfig: Cluster): (Raft.State, Raft.Event) => Raft.State = (state: State, evt: Event) => state.eventHandler(clusterConfig, evt)

}