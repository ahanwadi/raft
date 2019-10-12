package raft

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, WordSpecLike}
import raft.Raft.LogIndex

abstract class UnitSpec
    extends ScalaTestWithActorTestKit()
    with WordSpecLike
    with BeforeAndAfter {}
