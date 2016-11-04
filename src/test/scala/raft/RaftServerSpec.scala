package raft

import org.scalatest._
import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.testkit.{ TestActors, TestKit, ImplicitSender }
import org.scalatest.WordSpecLike
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RaftServerSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll with Eventually {


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A RaftServer actor" must {
    "start in follower mode" in {
      val raftServer = system.actorOf(Props[RaftServer])
      raftServer ! "getState"
      expectMsg( (0, None) )
    }
  }

  "A RaftServer actor" must {
    "eventually become candidate" in {
      val raftServer = system.actorOf(Props[RaftServer])
      eventually(timeout(10 seconds), interval(1 second)) {
        raftServer ! "getState"
        expectMsgPF() {
          case (_, Some(0)) => ()
        }
      }
    }
  }
}
