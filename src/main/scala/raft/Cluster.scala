package raft

import akka.actor.typed.ActorRef

/**
  * A cluster of raft server, each with an unique ID.
  */
trait Cluster {

  def myId: Raft.ServerId

  def members: Set[Raft.ServerId] = Set(myId)

  def memberRefs: Map[Raft.ServerId, ActorRef[Raft.RaftCmd]] = Map()

  def memberRef(id: Raft.ServerId): ActorRef[Raft.RaftCmd] = memberRefs(id)

  def quorumSize: Int = (clusterSize / 2) + 1

  def clusterSize: Int = members.size

}

/**
  * Default singleton cluster.
  */
object Cluster {
  def apply() = new Cluster {
    override def myId = Raft.ServerId(1)
  }

  def apply(id: Raft.ServerId) = new Cluster {
    override def myId = id
  }
}
