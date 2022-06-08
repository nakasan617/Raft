# Raft

This is a brief implementation of Raft which is a fault tolerant consensus algorithm that is easier to understand than paxos. Paxos are notoriously difficult to understand, which is developed by Leslie Lamport. To tackle with its complexity, a grad student in Stanford came up with Raft, which is a 3 way commit algorithm which is easy to understand.

# How to Run

What you need to have downloaded is scala and sbt.
Then all you need to do is pick the test you want to run and press the number and press enter. (The number below is not necessarily correct, but they are the tests).

# Explanation of Tests

1. MainTest: This is a test with test 2-4 combined.
2. LeaderElectionTest: This elects the leader after leader is dead.
3. LogCommitTest: This commits the message and checks if all the logs are the same between different nodes
4. ReviveTest: This is a test with after letting the leader die, then some commit of the messages, the former leader revives and restores the log. I used it to check if the former leader got all the logs evetually.
5. RecurringLeaderElectionTest: This kills 3 servers one of them being a leader. This would continuously create the reelection for the leader.
6. PartitionTest: This is a test that creates the partition. After the nodes are creates, the partition prevents them from communicating. Then I will log some messages in the majority side and see if they can remerge their logs back together.

# Some Implementation Details

- I have made the log replication after the revival happen not with HeartBeat, but to have the former dead realize with the heartbeat. This is because you need to implement whole a lot when you do just a heartbeat and it would make the code messy. Therefore, to emphasize the understandability, I have divided those functionalities.
- 3 phase commit is done on top of "fire and forget" protocol. This should suffice because you will do the exchange of the message to do the commit anyways (the majority has to acknowledge the precommit to commit the message). 
- I have reelectionIndex to help the servers distinguish when to vote. Without it, it cannot distinguish whether one needs to vote or it would be redundant.
- When the one that is late tries to catch up, I made the one who is trying to catch up almost all the work. The current leader just gives out the log of the specified index.