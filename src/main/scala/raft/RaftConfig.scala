package raft

import akka.actor.typed.{ActorSystem, Extension, ExtensionId}
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

class RaftConfig (config: Config) extends Extension {

  val raftConfig: Config = config.getConfig("raft")
  val electionTimeout: FiniteDuration =
    Duration.fromNanos(raftConfig.getDuration("election-timeout").toNanos)

}

object RaftConfig extends ExtensionId[RaftConfig] {
  override def createExtension(system: ActorSystem[_]): RaftConfig = new RaftConfig(system.settings.config)

  def get(system: ActorSystem[_]): RaftConfig = apply(system)
}