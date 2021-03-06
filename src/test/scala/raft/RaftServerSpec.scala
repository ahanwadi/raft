package raft

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RaftServerSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll with Eventually {


  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  "A RaftServer actor" must {
    "start in follower mode" in {
      val raftServer = system.actorOf(Props[RaftServer])
      raftServer ! RaftServer.GetState
      expectMsg( (0, None) )
    }
  }

  "A RaftServer actor" must {
    "eventually become candidate" in {
      val raftServer = system.actorOf(Props[RaftServer])
      eventually(timeout(10 seconds span), interval(1 second span)) {
        raftServer ! RaftServer.GetState
        expectMsgPF() {
          case (_, Some(0)) => ()
        }
      }
    }
  }

}
