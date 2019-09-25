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

  "LogIndex" must {
    "default logIndex must be up-to-date with itself" in {
      LogIndex() should be <= LogIndex()
      LogIndex() should be >= LogIndex()
    }

    "default logIndex must not be less than itself" in {
      LogIndex() shouldNot be < LogIndex()
    }
  }

}
