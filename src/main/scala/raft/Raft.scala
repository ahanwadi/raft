package raft

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
      case cmd @ RequestVote(term, candidate, _) if term > currentTerm =>
        context.log.info(s"Voting for $candidate in $term")
        Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
          state.enterMode(timers, context)
        }.thenReply(cmd) { _ =>
          Vote(term, myId, Some(candidate), true)
        }
      case e: Term if e.term > currentTerm =>
        Effect.persist(NewTerm(e.term)).thenRun { state =>
        state.enterMode(timers, context)
      }
      case e: Term if e.term < currentTerm => Effect.none
      case c: GetState =>
        c.sender ! CurrentState(myId, currentTerm, getMode)
        Effect.none
    }
  }

  final case class Follower(myId: Int, currentTerm: Int = 0, votedFor: Option[Int] = None) extends State {

    def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]): CommandHandler[RaftCmd, Event, State] = CommandHandler.command { case cmd =>

      cmd match {
        case AppendEntries(term, _, _) if term == currentTerm =>
          /* On heartbeat restart the timer, only if heartbeat is for the current term */
          /* TODO: Do we need to verify if heartbeat is from leader of the current term? */
          Effect.none.thenRun { state =>
            state.enterMode(timers, context)
          }
        case AppendEntries(term, _, _) if term > currentTerm =>
          // We just got a heartbeat from the new leader, we haven't voted.
          Effect.persist(NewTerm(term, None)).thenRun { state =>
            state.enterMode(timers, context)
          }
        case HeartbeatTimeout =>
          Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
            state.enterMode(timers, context)
          }
        case cmd @ RequestVote(term, candidate, _) if votedFor.isEmpty =>
          context.log.info(s"Voting for $candidate in $term")
          Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
            state.enterMode(timers, context)
          }.thenReply(cmd) { _ =>
            Vote(term, myId, Some(candidate), true)
          }
        case cmd @ RequestVote(term, _, _) =>
          Effect.none.thenRun { state: Raft.State =>
            state.enterMode(timers, context)
          }.thenReply(cmd) { _ =>
            Vote(term, myId, votedFor, false)
          }
        case _ => Effect.unhandled
      }
    }

    override def getMode: String = "Follower"
  }

  final case class Candidate(myId: Int, currentTerm: Int = 0, votedFor: Option[Int]) extends State {

    override def commandhandler(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = CommandHandler.command { cmd =>
      cmd match {
        case AppendEntries(term, _, _) =>
          /* Transition to follower. What if term is same currentTerm? and
           * the leader crashes? In that case, entire existing quorum set of
           * servers won't be transitioning to candidate themselves and they
           * won't vote for us either or anyone else. In that case, election
           * will timeout and goto next term.
           */
          Effect.persist(NewTerm(term, None)).thenRun { state =>
           state.enterMode(timers, context)
          }
        case cmd @ RequestVote(term, candidate, _) if term > currentTerm =>
          context.log.info(s"Got request vote with higher term - $term than $currentTerm")
          Effect.persist(NewTerm(term, Some(candidate))).thenRun { state: Raft.State =>
            state.enterMode(timers, context)
          }.thenReply(cmd) { _ =>
            Vote(term, myId, Some(candidate), true)
          }
        case cmd @ RequestVote(term, _, _) =>
          Effect.none.thenRun { state: Raft.State =>
            state.enterMode(timers, context)
          }.thenReply(cmd) { _ =>
            context.log.info(s"Rejecting vote in the same term - $term ($currentTerm) as a candidate")
            Vote(term, myId, votedFor, false)
          }
        case HeartbeatTimeout =>
          Effect.persist(NewTerm(currentTerm + 1, Some(myId))).thenRun { state =>
            state.enterMode(timers, context)
          }
        case _ => Effect.none
      }
    }

    override def getMode: String = "Candidate"
  }

  final case class Leader(myId: Int, currentTerm: Int) extends State {

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

  final case class NewTerm(term: Int, votedFor: Option[Int] = None) extends Event


  sealed trait RaftCmd
  final case class RequestVote[Reply](term: Int, candidate: Int, override val replyTo: ActorRef[RaftCmd]) extends RaftCmd with Term with ExpectingReply[Vote]
  final case class Vote(term: Int, voter: Int, votedFor: Option[Int], result: Boolean) extends RaftCmd with Term
  final case class AppendEntries(term: Int, leader: Int, from: ActorRef[RaftCmd]) extends RaftCmd with Term
  final case class HeartbeatResponse(term: Int, id: Int) extends RaftCmd with Term
  final case object HeartbeatTimeout extends RaftCmd

  sealed trait ClientProto extends RaftCmd
  final case class GetState(sender: ActorRef[ClientProto]) extends ClientProto

  final case class CurrentState(id: Int, term: Int, mode: String) extends ClientProto

  def startHeartbeatTimer(timers: TimerScheduler[RaftCmd], context: ActorContext[RaftCmd]) = {
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val electionTimeout: FiniteDuration = Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos())

    timers.startSingleTimer(HeartbeatTimeout, HeartbeatTimeout, electionTimeout)
  }

  def apply(id: Option[Int] = None): Behavior[RaftCmd] = Behaviors.setup { context: ActorContext[RaftCmd] =>
    val raftConfig: Config = context.system.settings.config.getConfig("raft")
    val myId: Int = id.getOrElse(raftConfig.getInt("myId"))
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
      case NewTerm(term, votedFor) if votedFor == Some(state.myId) => Candidate(state.myId, term, votedFor)
      case NewTerm(term, votedFor) => Follower(state.myId, term, votedFor)
    }
  }

}
