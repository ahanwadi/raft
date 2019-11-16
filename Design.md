ReplicatedLog - a Persistent typed actor that manages the log. Log entries are indexed by logIndex and have term. Each log stores a command (type parameter).
    It has two parts.
        Maintains the log as a series of log entries as a pure persistent state machine with no side-effects. A log is modified when new entries are appended or trimmed. Also, manages commitIndex based on replication status.
        Handles replication of log to the quorum of raft servers and then computes/updates the commitIndex. Receives log entries from leader.

               eventHandler -> AppendEntries(logs) just appends a log entries to the log.
                            -> ConflictingEntries(index) - delete conflicting entries.

               command handler -> ReplicateLog - generate AppendEntries and then send it to quorum
                                  Reply -> quorum? update nextIndex and matchIndex for the follower. Compute commitIndex and let statemachine know the commitIndex.
                                  AppendEntries(logs, prevLog) -> update entries after trimming mis-matched entries. To trim entries we generate a ConflictingEntries(startingIndex) evt, followed by correct log entries received from the leader.
                                  Timeout -> if leader -> Send AppendEntries to all followers based on their nextIndex


StateMachine - Applies series of commands in order from the log upto commitIndex. Uses ReplicatedLog to replicate the log entries and receive commitIndex.

                commandhandler -> SetValue. ask ReplicatedLog -> ReplicateLog
                                  Commit(commitIndex) - apply entries from appliedIndex to commitIndex and update appliedIndex. Let raftServer know updated appliedIndex.
                eventhandler -> ApplyCmd(cmd) - update the state based on the cmd.

Cluster - A cluster of raft servers - maintains membership and quorum information.

RaftServer - A single raft server - each server has an unique serverId. Participates and conducts leader election. Accepts commands (type parameter) from user. Maintains mapping of logIndex -> client. When appliedIndex is updated by the SM, it responds to all pending clients upto appliedIndex.

A raft client sends commands to the current leader, which replicates the entries to the quorum and if successful, commits the entry and responds to the client.

