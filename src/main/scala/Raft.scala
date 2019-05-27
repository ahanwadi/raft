package raft

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import com.typesafe.config.Config
import scala.concurrent.duration.{Duration, FiniteDuration}

object Raft {

  sealed trait ServerId {
    def myId: Int
  }

  sealed trait Term {
    def term: Int
  }

  type Index = Int

  // The persistent state stored by all servers.
  sealed trait State {
    def myId: Int
    def currentTerm: Int

    def getMode: String

    def enterMode(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = {
      startHeartbeatTimer(timers, context)
    }

    def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]): CommandHandler[RaftCmd, Raft.Event, Raft.State]

    def defhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) : PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = {
      case e: Term if e.term > currentTerm => Effect.persist(NewTerm(e.term))
      case e: Term if e.term < currentTerm => Effect.none
      case c: GetState =>
        c.sender ! CurrentState(myId, currentTerm, getMode)
        Effect.none
    }
  }

  case class Follower(myId: Int, currentTerm: Int = 0, votedFor: Option[ServerId] = None) extends State {

    def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]): CommandHandler[RaftCmd, Event, State] = CommandHandler.command { case cmd =>

      cmd match {
        case heartbeat@AppendEntries(term, leader, _) =>
          /* On heartbeat restart the timer, only if heartbeat is for the current term */
          /* TODO: Do we need to verify if heartbeat is from leader of the current term */
          Effect.none.thenRun { state =>
            state.enterMode(timers, context)
          }
        case HeartbeatTimeout =>
          Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
            state.enterMode(timers, context)
          }
        case _ => Effect.unhandled
      }
    }

    override def getMode: String = "Follower"
  }

  case class Candidate(myId: Int, currentTerm: Int = 0, votedFor: Option[Int]) extends State {

    override def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = CommandHandler.command { cmd =>
      cmd match {
        case heartbeat @ AppendEntries(term, leader, _) => Effect.none
        case HeartbeatTimeout => Effect.persist(NewTerm(currentTerm + 1, Some(myId)))
        case _ => Effect.none
      }
    }

    override def getMode: String = "Candidate"
  }

  case class Leader(myId: Int, currentTerm: Int) extends State {

    override def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = CommandHandler.command { cmd =>
      cmd match {
        case HeartbeatTimeout => Effect.none // Send heartbeat
        case _ => Effect.none
      }
    }

    override def getMode: String = "Leader"
  }

  sealed trait Event {
    def term: Int
  }

  case class NewTerm(term: Int, votedFor: Option[Int] = None) extends Event


  sealed trait RaftCmd
  case class RequestVote(term: Int, candidate: ServerId, from: ActorRef[RaftCmd]) extends RaftCmd with Term
  case class Vote(term: Int, voter: ServerId, votedFor: Option[ServerId], result: Boolean) extends RaftCmd with Term
  case class AppendEntries(term: Int, leader: ServerId, from: ActorRef[RaftCmd]) extends RaftCmd with Term
  case class HeartbeatResponse(term: Int, id: ServerId) extends RaftCmd with Term
  case object HeartbeatTimeout extends RaftCmd

  sealed trait ClientProto extends RaftCmd
  case class GetState(sender: ActorRef[ClientProto]) extends ClientProto

  case class CurrentState(id: Int, term: Int, mode: String) extends ClientProto

  def startHeartbeatTimer(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = {
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

    timers.startSingleTimer(HeartbeatTimeout, HeartbeatTimeout, electionTimeout)
  }

  def apply(): Behavior[RaftCmd] = Behaviors.setup { context: ActorContext[RaftCmd] =>
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val myId: Int = raftConfig.getInt("myId")
    val persistenceId = PersistenceId(s"raft-server-typed-$myId")

    Behaviors.withTimers { timers: TimerScheduler[RaftCmd] =>
      EventSourcedBehavior[RaftCmd, Event, State](
        persistenceId = persistenceId,
        emptyState = Follower(myId),
        commandHandler = dispatcher(timers, context),
        eventHandler = eventHandler
      ).receiveSignal {
          case (state, RecoveryCompleted) => state.enterMode(timers, context)
        }

    }
  }

  def dispatcher(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) : (Raft.State, RaftCmd) => Effect[Raft.Event, Raft.State] = {
    (state: Raft.State, cmd: RaftCmd) => {
      def secondhandler: PartialFunction[RaftCmd, Effect[Raft.Event, Raft.State]] = { case c =>
          state.commandhandler(timers, context)(state, c)
      }
      val y: Effect[Event, State] =
      state.defhandler(timers, context = context).orElse(secondhandler)(cmd)
      y
    }
  }


  def eventHandler: (Raft.State, Raft.Event) => Raft.State = (state: State, evt: Event) => {
    evt match {
        /*
         * When votedFor == empty or votedFor != myId became follower
         * Else goto Candidate.
         */
      case NewTerm(term, Some(votedFor)) if votedFor == state.myId => Candidate(state.myId, term, Some(votedFor))
      case NewTerm(term, _) => Follower(state.myId, term)
    }
  }


}
