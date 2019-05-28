package raft

import akka.actor.typed.ActorRef
import raft.Raft.RaftCmd


/*
 * Provides membership for the raft protocol.
 */
trait Membership {
  def getMembers: List[ActorRef[RaftCmd]]

}