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
import com.typesafe.config._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RaftFSMTest() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll with Eventually {


  private var raftActors: Array[akka.actor.ActorRef] = null

  val raftConfig: Config = system.settings.config.getConfig("raft")

  lazy val raftServers = raftConfig.getObjectList("servers").asScala.toList
    .map {_.toConfig()}
    .map { srvr => RaftFSM.Server(srvr.getInt("id"), srvr.getString("address"))}

  object SetExtractor {
    def unapplySeq[T](s: Set[T]): Option[Seq[T]] = Some(s.toSeq)
  }

  override def beforeAll {
    raftActors = raftServers.map { server =>
      system.actorOf(Props(classOf[RaftFSM], server.id))
    }.toArray
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A RaftServer actor" must {
    "start in follower mode" in {
      raftActors foreach { raftServer => raftServer ! RaftFSM.GetState }
      eventually(timeout(10 seconds), interval(1 second)) {
        raftServers foreach { _ =>
          expectMsg( RaftFSM.ServerData(0, None, Set()) )
        }
      }
    }
  }

  /*
  "A RaftServer actor" must {
    "eventually become candidate" in {
      raftActors foreach { raftServer => raftServer ! RaftFSM.GetState }
      raftServers foreach { server =>
        eventually(timeout(10 seconds), interval(1 second)) {
          expectMsgPF() {
            case RaftFSM.ServerData(_, Some(server.id), y) if y.contains(server.id) => ()
          }
        }
      }
    }
  }
   */

  "The RaftServer actor" must {
    "eventually become leader" in {
      eventually(timeout(30 seconds), interval(2 second)) {
        raftActors foreach { raftServer => raftServer ! RaftFSM.IsLeader }
        expectMsgAllOf("Leader", "Follower", "Follower")
      }
    }
  }

}
