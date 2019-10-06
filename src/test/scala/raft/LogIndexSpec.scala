package raft

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, WordSpecLike}
import raft.Raft.LogIndex

@RunWith(classOf[JUnitRunner])
class LogIndexSpec
    extends ScalaTestWithActorTestKit()
    with WordSpecLike
    with BeforeAndAfter {

  "default LogIndex" must {
    "up-to-date with itself" in {
      LogIndex() should be <= LogIndex()
      LogIndex() should be >= LogIndex()
    }

    "must not be less than itself" in {
      LogIndex() shouldNot be < LogIndex()
    }
  }

}
