package raft

import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import raft.Raft._
import raft.Cluster
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util._

/**
  * An actor that continuously replicates the log entries from leader to all its
  * followers.
  */
object Replicator {

  sealed trait ReplicatorMsg

  final case class ReplicationTimeout(follower: ServerId) extends ReplicatorMsg
  final case class Replicated(follower: ServerId, result: Boolean)
      extends ReplicatorMsg
  final case class LogAppended(entry: Log, client: ActorRef[ClientReply]) extends ReplicatorMsg

  def apply(
      currentTerm: Int,
      parent: ActorRef[RaftCmd],
      clusterConfig: Cluster,
      rsm: Logs
  ) = replicator(currentTerm, parent, clusterConfig, rsm)

  /*
   * Replicator is started when a leader is elected. On startup, it replicates a no-op entry
   * and thus ensures all log entries in previous terms are fully replicated. Only after that
   * the leader will accept client requests.
   *
   * For each client request accepted by the leader, leader asks replicator to replicate the entry
   * After it is replicated to a quorum, it lets leader know that it can be committed.
   * Only committed entries are ack-ed to the client.
   */
  def replicator(
      currentTerm: Int,
      parent: ActorRef[RaftCmd],
      clusterConfig: Cluster,
      rsm: Logs,
      matchIndex: Map[ServerId, Index] = Map(),
      nextIndex: Map[ServerId, Index] = Map(),
      clients: Map[Index, ActorRef[ClientReply]] = Map()
  ): Behavior[ReplicatorMsg] =
    Behaviors.setup[ReplicatorMsg] { context =>
      /*
       * Start replicating entries to followers.
       * For each follower we maintain nextIndex where to start
       * replicating logs from and matchIndex - index which
       * upto which a follower's logs match with this leader.
       */
      clusterConfig.otherMembers.foreach { followerId =>
        sendAppendEntries(followerId)
      }

      def sendAppendEntries(followerId: ServerId): Unit =  {
        val nextIndexWithDef = nextIndex.withDefaultValue(rsm.lastLogIndex().index)
        val followerRef = clusterConfig.memberRef(followerId)
        val prevLogIndex = nextIndexWithDef(followerId) - 1
        val logsToSend =
          rsm.logs.slice(nextIndexWithDef(followerId).idx, rsm.logs.length)

        implicit val timeout: Timeout = 3.seconds
        if (rsm.lastLogIndex().index >= nextIndexWithDef(followerId)) {
          context.log.info(s"Replicating to ${followerId} at ${prevLogIndex}")
          context.ask(followerRef) { ref =>
            AppendEntries(
              currentTerm,
              clusterConfig.myId,
              ref,
              rsm.logs(prevLogIndex.idx).index,
              rsm.committed,
              logsToSend
            )
          } {
            case Success(RaftReply(currentTerm, followerId, _, result)) =>
              Replicated(followerId, result)
            case Failure(x) => ReplicationTimeout(followerId)
          }
        } else {
          context.log.info(s"Skipping replicating to ${followerId} at ${prevLogIndex}")
        }
      }

      /**
        * Compute which logs have successfully replicated to a quorum of followers
        * and hence can be committed safely.
        */
      def findCommitIndex(
          rsm: Logs,
          quorumSize: Int,
          matchIndex: Map[ServerId, Index] = Map()
      ): Index = {

        val minIdx = math.max(0, rsm.committed.idx - 1)
        /*
         If there exists an N such that N > commitIndex, a majority
         of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
         */
        val newCommitIndex =
          ((rsm.logs.length - 1) to minIdx by -1).find { i =>
            matchIndex.values.count { x: Index =>
              (x.idx - 1) >= i && rsm.logs(x.idx - 1).index.term == currentTerm
            } >= quorumSize
          }
        val logStr = rsm.logs.mkString("\n")
        context.log.debug(s"${logStr} - Committed - ${rsm.committed} - ${newCommitIndex}")
        if (newCommitIndex.isDefined) {
          return Index(newCommitIndex.get + 1)
        } else {
          return rsm.committed
        }
      }

      Behaviors.receiveMessage {
        case Replicated(follower, result) =>
          if (result) {

            val matchIdx = matchIndex + (follower -> rsm.lastLogIndex())

            val leaderCommit =
              findCommitIndex(rsm, clusterConfig.quorumSize, matchIndex)

            context.log.info(s"Successfully replicated to ${follower} - ${leaderCommit} - ${rsm.lastLogIndex} ")

            val clnts = clients filter { case (index, client) =>
              val done = leaderCommit >= (index - 1)
              if (done) {
                parent ! Raft.ClientReplicated(index, client)
              }
              done
            }

            if (leaderCommit > rsm.committed) {
              parent ! Committed(leaderCommit)
            }

            replicator(
              currentTerm,
              parent,
              clusterConfig,
              rsm.copy(committed = leaderCommit),
              matchIndex + (follower -> rsm.lastLogIndex().index),
              nextIndex + (follower -> (rsm.lastLogIndex().index + 1)),
              clnts
            )

          } else {
            replicator(
              currentTerm,
              parent,
              clusterConfig,
              rsm,
              matchIndex,
              nextIndex.updatedWith(follower) { _.map(_ - 1) }
            )
          }
        case ReplicationTimeout(follower) =>
          // Try sending AppendEntries to the follower on timeout
          sendAppendEntries(follower)
          Behavior.same

        case LogAppended(entry, client) =>
          context.log.info(s"Appending ${entry}")
          replicator(
            currentTerm,
            parent,
            clusterConfig,
            rsm.copy(logs = rsm.logs :+ entry),
            matchIndex,
            nextIndex.withDefaultValue(rsm.lastLogIndex().index),
            clients + (entry.index.index -> client)
          )
        case _ => Behaviors.unhandled
      }
    }
}
