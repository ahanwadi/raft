package raft

import akka.actor.typed.ActorRef

trait Cluster {

  def myId: Int

  def members: Set[Int] = Set(myId)

  def memberRefs: Map[Int, ActorRef[Raft.RaftCmd]] = Map()

  def memberRef(id: Int): ActorRef[Raft.RaftCmd] = memberRefs(id)

  def quorumSize: Int = (clusterSize / 2) + 1

  def clusterSize: Int = members.size

}

object Cluster {
  def apply() = new Cluster {
    override def myId = 1
  }

  def apply(id: Int) = new Cluster {
    override def myId = id
  }
}


