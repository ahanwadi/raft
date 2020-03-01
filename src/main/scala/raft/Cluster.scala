package raft

import akka.actor.typed.ActorRef

/**
  * A cluster of raft server, each with an unique ID.
  */
trait Cluster {

  def myId: Raft.ServerId

  def members: Set[Raft.ServerId] = Set(myId)

  def otherMembers: Set[Raft.ServerId] = members - myId

  def memberRefs: Map[Raft.ServerId, ActorRef[Raft.RaftCmd]] = Map()

  def memberRef(id: Raft.ServerId): ActorRef[Raft.RaftCmd] = memberRefs(id)

  def quorumSize: Int = (clusterSize / 2) + 1

  def clusterSize: Int = members.size

}

/**
  * Default singleton cluster.
  */
object Cluster {
  def apply(): Cluster = new Cluster {
    override def myId: Raft.ServerId = Raft.ServerId(1)
  }

  def apply(id: Raft.ServerId): Cluster = new Cluster {
    override def myId: Raft.ServerId = id
  }
}
