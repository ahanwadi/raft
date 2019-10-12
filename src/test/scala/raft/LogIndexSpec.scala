package raft

import raft.Raft.LogIndex

class LogIndexSpec extends UnitSpec() {

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
