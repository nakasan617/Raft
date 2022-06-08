import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import akka.util.Timeout

import java.util.concurrent.TimeoutException

case object Start
case object CheckSelf
case object Die
case object Revive
case object ShowLog
case object GoLeft
case object AskLeader
case class HeartBeat(theirTerm: Int, theirLastLog: Log)
case class AddServer(server: ActorRef)
case class AddServers(servers: HashSet[ActorRef])
case class VoteForMe(theirTerm: Int, theirRI: Int)
case class Vote(theirTerm: Int)
case class LeaderNotify(theirTerm: Int, newLeader: ActorRef)
case class Log(ref: ActorRef, message: String, Id: Int)
case class LogServer(message: String)
case class LogReplication(log: Log, theirTerm: Int)
case class AckPrecommit(log: Log)
case class Commit(log: Log)
case class CatchUp(theirIndex: Int)
case class Partition(part: HashSet[ActorRef])
case class Unpartition(all: HashSet[ActorRef])

class Server(var role: String) extends Actor {
  var leader: ActorRef = null
  var alive: Boolean = true
  val constantTime: Int = 500
  val otherServers: HashSet[ActorRef] = new HashSet()
  var cancellable: Cancellable = null
  val system = context.system
  val r = scala.util.Random
  var term: Int = 0
  var reelectionIndex: Int = 0
  var numVotes: Int = 0
  var logs: Array[Log] = Array()
  var preCommit: HashMap[Log, Int] = new HashMap()
  var logId: Int = 0
  var numNodes: Int = 1
  var lastCommittedLog: Log = null

  def receive = {
    case message: String => println(message)
    case Start =>
      if(role == "leader") sendHeartBeats()
      else waitForHeartBeat()
    case HeartBeat(theirTerm: Int, theirLastLog: Log) => if(alive) receiveHeartBeat(theirTerm, theirLastLog)
    case AddServer(server: ActorRef) => if(alive) addServer(server)
    case AddServers(servers: HashSet[ActorRef]) => if(alive) addServers(servers)
    case VoteForMe(theirTerm: Int, theirRI: Int) => if(alive) voteForMe(theirTerm, sender(), theirRI: Int)
    case CheckSelf => if(alive) sender() ! "OK"
    case Vote(theirTerm: Int) => if(alive) voteReceived(theirTerm: Int)
    case LeaderNotify(theirTerm: Int, newLeader: ActorRef) => if(alive) leaderNotify(theirTerm: Int, newLeader)
    case Die => if(alive) die()
    case Revive => if(!alive) revive()
    case LogServer(message) => if(alive) logServer(message)
    case LogReplication(log: Log, theirTerm: Int) => if(alive) logReplication(log, theirTerm)
    case AckPrecommit(log: Log) => if(alive) ackPrecommit(log)
    case Commit(log: Log) => if(alive) commit(log)
    case ShowLog => if(alive) sender() ! logs
    case CatchUp(theirIndex: Int) => if(alive) catchUpLeader(theirIndex)
    case AskLeader => if(alive) askLeader()
    case Partition(part: HashSet[ActorRef]) => if(alive) partition(part)
    case Unpartition(all: HashSet[ActorRef]) => if(alive) unpartition(all)
  }

  /*
  this is the method for the leader
  it sends the heart beat to each followers
   */
  def sendHeartBeats() = {
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleWithFixedDelay(0.seconds, 200.millis) { () =>
      implicit val timeout = Timeout(5.seconds)
      otherServers.foreach((server: ActorRef) => server ! HeartBeat(term, lastCommittedLog))
      try {
        val future = ask(self, CheckSelf)
        Await.result(future, 50.millis)
        if (future == null) {
          cancellable.cancel()
        }
      } catch {
        case te: TimeoutException => cancellable.cancel()
      }
    }
  }

  /*
  this method waits for the heart beat for each interval,
  if it doesn't get the heart beat,
  it starts the election
  */
  def waitForHeartBeat() = {
    lastCommittedLog = Log(sender(), "", logId)
    val time = r.nextInt(300) + constantTime
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(time.millis) {
      implicit val timeout = Timeout(5.seconds)
      startElection()
    }
  }

  /*
  This is what we do when we receive the heart beat,
  we cancel the scheduled election and start a new schedule
  Whenever it receives the HeartBeat and notices that the node is late,
  it will do the catching up with latest logs with the current leader
   */
  def receiveHeartBeat(theirTerm: Int, theirLastLog: Log) = {
    if (term == theirTerm) {
      if(lastCommittedLog == theirLastLog) {
        if (cancellable != null) {
          //      println("cancelled at :" + self.path.toString)
          cancellable.cancel()
        } else {
          //println("cancellable was null")
        }
        val time = r.nextInt(300) + constantTime
        implicit val ec = system.dispatcher
        cancellable = system.scheduler.scheduleOnce(time.millis) {
          implicit val timeout = Timeout(5.seconds)
          startElection()
        }
      } else {
        println("last committed log was different: " + self.path.toString)
        catchUpFollower(theirTerm)
      }
    } else if(term < theirTerm) {
      // I am late, so you need to catch up with the leader (sender is the leader here)
      catchUpFollower(theirTerm)
    } else {
      // you just ignore if the leader is late (the older leader will get the heart beat from the new leader eventually)
    }
  }

  /*
  This method is called by the follower when they figure out that the log is different from the ones that's supposed to be
  This method is called when the heartBeat is received and the follower realizes the differences between the
   */
  private def catchUpFollower(theirTerm: Int): Unit = {
    val oldTerm = term
    role = "follower"
    term = theirTerm
    var caughtUp = false
    var left = true
    var currIndex: Int = logs.size - 1

    implicit val timeout = Timeout(1.seconds)

    while(!caughtUp) {
      var future:Future[Log] = null
      try {
        future = ask(sender(), CatchUp(currIndex)).mapTo[Log]
        Await.result(future, timeout.duration)
      } catch {
        case e: TimeoutException => {
          term = oldTerm
          println("timed out exception, we need to redo this")
          caughtUp = true // break from the loop and hope for the next opportunity
        }
      }
      val value: Log = future.value.get.get
      if(value.message == "dummy") {
        if(left) {
          if(currIndex < 0) {
            currIndex = 0
            left = false
          } else {
            currIndex -= 1
          }
        } else {
          println(self.path.toString + " is caught up")
          lastCommittedLog = logs(logs.size - 1)
          caughtUp = true
        }
      } else { // this is when you get the actual value
        if(left) {
          if(logs(currIndex) == value) {
            left = false
            currIndex += 1
          } else {
            currIndex -= 1
          }
        } else {
          if(currIndex < logs.size) {
            logs(currIndex) = value
            currIndex += 1
          } else if(currIndex == logs.size) {
            logs :+= value
            currIndex += 1
          } else {
            println("not right")
            system.terminate()
          }
        }
      }
    }
  }

  /*
  this methods adds other servers
   */
  def addServer(server: ActorRef) = {
    otherServers += server
    numNodes += 1
  }

  /*
  this methods adds HashSet of servers
   */
  def addServers(servers: HashSet[ActorRef]) = {
    otherServers ++= servers
    otherServers -= self
    numNodes = otherServers.size + 1
  }

  /*
  this method is what the follower does when the leader dies and new node has to be elected
  numVotes starts out as one because you vote for yourself
   */
  def startElection() = {
    //term += 1
    reelectionIndex += 1
    println("did not receive the heartBeat, starting leader election: " + self.path.toString + ", now at term " + term + " at reelectionIndex: " + reelectionIndex)
    role = "candidate"
    numVotes = 1

    val time = r.nextInt(400) + 4000
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(time.millis) {
      implicit val timeout = Timeout(5.seconds)
      restartElection()
    }

    otherServers.foreach((server: ActorRef) => server ! VoteForMe(term, reelectionIndex))
  }

  /*
  This method restarts the election in case the leader was not chosen in time
   */
  private def restartElection(): Unit = {
    reelectionIndex += 1
    numVotes = 1
    role = "candidate"
    println("could not elect the leader, will restart: " + self.path.toString + " at term " + term + " at reelectionindex: " + reelectionIndex)
    scheduleRestartElection()
    otherServers.foreach((server: ActorRef) => server ! VoteForMe(term, reelectionIndex))
  }

  /*
  This is a method to save some redundant writing of creating a scheduler for restarting election
   */
  private def scheduleRestartElection(): Unit = {
    val time = r.nextInt(400) + 4000
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(time.millis) {
      implicit val timeout = Timeout(5.seconds)
      restartElection()
    }
  }

  /*
  This method is a response of the FOLLOWER (NOT leader) to either vote for the guy, or not vote for the guy
  if the follower is voting there must be a line candidate ! Vote
   */
  def voteForMe(theirTerm: Int, candidate: ActorRef, theirRI: Int) = {
    //println(self.path.toString + ", role: " + role + ", term: " + term + ", RI: " + reelectionIndex)
    if(role == "follower") {
      if(theirTerm > term) {
        if(cancellable != null) {
          cancellable.cancel()
        }

        //println("I am " + self.path.toString + ", voting for " + candidate.path.toString + " in term " + theirTerm)
        reelectionIndex = theirRI
        scheduleRestartElection()
        candidate ! Vote(theirTerm)
        term = theirTerm
      } else if(theirTerm == term) {
        if(reelectionIndex < theirRI) { // if they are ahead of us, vote
//          println(self.path.toString + " theirRI is bigger with the same term, voting for " + candidate.path.toString)
          reelectionIndex = theirRI
          if(cancellable != null) {
            cancellable.cancel()
          }
          scheduleRestartElection()
          candidate ! Vote(term)
        } else {
          // if not, don't do anything
        }
      } else {
        // term > theirTerm
        // you should not vote in this case
      }
    } else if (role == "leader") {
      if(theirTerm > term) { // if they are ahead
        role = "follower"
        if(cancellable != null) {
          cancellable.cancel()
        }
        scheduleRestartElection()
        candidate ! Vote(theirTerm)
        term = theirTerm
      } else if(theirTerm < term) {
        // this guy will eventually give out the heartBeat to check the term
      } else {
        println("The term is the same and you guys are candidate and leader, this can't be happening")
      }
    } else {
      assert(role == "candidate")
      if(theirTerm == term) {
        if(reelectionIndex < theirRI) {
          // if they are ahead, you should vote
          role = "follower"
          reelectionIndex = theirRI
          if(cancellable != null) {
            cancellable.cancel()
          }
          scheduleRestartElection()
          candidate ! Vote(term)
        } else {
          // don't vote in this case
        }
      } else if (theirTerm > term) {
        // I need to follow them
        role = "follower"
        term = theirTerm
        reelectionIndex = theirRI
        if(cancellable != null) {
          cancellable.cancel()
        }
        scheduleRestartElection()
        candidate ! Vote(theirTerm)
      } else {
        // they need to follow me
        assert(theirTerm < term)
        // you don't need to do anything here
      }
    }
  }

  /*
  This methods is called when a vote is received form a follower, and when one gets the number of vote that is more than half the number of servers,
  it should notify all the followers that it became the leader and starts sending HeartBeats
   */
  def voteReceived(theirTerm: Int): Unit = {
    numVotes += 1
    val half: Int = numNodes / 2
    if (numVotes > half && term == theirTerm) {
      if(cancellable != null) {
        cancellable.cancel()
      }
      term += 1
      println("leader elected, " + self.path.toString + " is the new leader at term " + term)
      reelectionIndex = 0
      otherServers.foreach((server: ActorRef) => {
        implicit val timeout = Timeout(1.seconds)
        try {
          val future = ask(server, LeaderNotify(term, self)).mapTo[String]
          Await.result(future, timeout.duration)
          if (future == null || future.value == null) {
            println("leader notify did not work")
            system.terminate() // this is basically asserting
          } else if (future.value.get.get == "No Good") {
            println("there is a problem with the term")
            system.terminate() // this is basically asserting for now
          } else if (future.value.get.get == "OK") {
            //println("OK was received")
          } else {
            println("some random messages received: " + future.value.get.get)
          }
        } catch {
          case te: java.util.concurrent.TimeoutException => {
            println("timeout exception caught when sending it to " + server.path.toString)
          }
        }
      })
      role = "leader"
      leader = self
      sendHeartBeats()
    }

  }

  /*
  This method is used when they know the new leader and the leader notifies the followers
  The new leader informs the followers to update their terms
   */
  def leaderNotify(theirTerm: Int, newLeader: ActorRef): Unit = {
    if(theirTerm >= term) {
      reelectionIndex = 0
      cancellable.cancel()
      sender() ! "OK"
      term = theirTerm
      role = "follower"
      leader = newLeader
    } else {
      sender() ! "No Good"
    }
  }

  /*
  This method kills the actor, and cancels any scheduled procedures
   */
  def die(): Unit = {
    println(self.path.toString + " dies. Bye.")
    alive = false
    cancellable.cancel()
    cancellable = null
  }

  /*
  This method revives the actor
   */
  def revive(): Unit = {
    println(self.path.toString + " revived")
    alive = true
  }

  /*
  This method is what logging of what leader does.
  if you are not the leader, forward the message to the leader
  if you are the leader, add the entry and send the log to the followers, when you get the message back from the majority, commit
   */
  def logServer(message: String) = {
    if(role != "leader") {
      if(leader != null) {
        //println("forwarding the message: " + message + " to " + leader.path.toString)
        leader ! LogServer(message)
      } else {
        println("leader does not exist, aborting the message: " + message)
      }
    } else {
      // add the entry
      //println("I am the leader: " + self.path.toString + ", let's log the message: " + message)
      val log = Log(self, message, logId)
      preCommit(log) = 1
      logs = logs :+ log
      logId = logId + 1
      otherServers.foreach((server: ActorRef) => server ! LogReplication(log, term))
    }
  }

  /*
  This method is used when the follower receives the precommit message as well as its logs
  The term has to be the same to do the replication and if the term is not right, it is ignored
   */
  def logReplication(log: Log, theirTerm: Int): Unit = {
    if(role != "follower") {
      //println("I am at logReplication, role: " + role + ", " + self.path.toString + ", theirTerm: " + theirTerm + ", myTerm: " + term)
    } else {
      if (theirTerm == term) {
        //println("precommiting at " + self.path.toString + " by " + log.message)
        preCommit(log) = 0
        sender() ! AckPrecommit(log)
      } else {
        println("term is different between " + self.path.toString + ":" + term + ", " + sender().path.toString + ":" + theirTerm)
        // just ignore in this case
      }
    }
  }

  /*
  This method is used by the leader when the follower sends the acknowledgement for the precommit
  You will store the number of acknowledge in the HashMap, and when it is over half, it is removed from the hashmap, added to the log,
  and sends messages to the followers to commit as well
   */
  def ackPrecommit(log: Log): Unit = {
    assert(role == "leader")
    if (preCommit.contains(log)) {
      //println("precommit for " + log.message + " by " + sender().path.toString)
      preCommit(log) += 1

      if ((numNodes) / 2 < preCommit(log) && preCommit(log) <= (numNodes) / 2 + 1) {
        println("commiting " + log)
        preCommit -= (log)
        lastCommittedLog = log
        otherServers.foreach((server: ActorRef) => server ! Commit(log))
      }
    }
  }

  /*
  This method commits the message to the followers, I should check the term here
   */
  def commit(log: Log) = {
    if(preCommit.contains(log) && role == "follower") {
      logs = logs :+ log
      lastCommittedLog = log
    } else {
      //println("commit message for log that is not in precommit: " + log.message + ", " + log.ref.path.toString + ", " + log.Id)
    }
  }

  /*
  This method is used by the leader to reply the request from the follower to see the logs.
  It simply gives back the logs in the certain index, if not available, gives back dummy log
   */
  def catchUpLeader(theirIndex: Int): Unit = {
    assert(role == "leader")
    if(0 <= theirIndex && theirIndex < logs.size) {
      sender() ! logs(theirIndex)
    } else {
      sender() ! Log(self, "dummy", -1)
    }
  }

  /*
  returns who the leader is
   */
  def askLeader()= {
    sender() ! leader
  }

  /*
  prints out the logs
   */
  private def printLogs(): Unit = {
    for(x <- logs) {
      print(x)
      print(", ")
    }
    println()
  }

  /*
  This method creates a partition between the servers
   */
  def partition(part: HashSet[ActorRef]): Unit = {
    if(part.contains(self)) {
      val res = otherServers.diff(part)
      otherServers --= res
    } else {
      otherServers --= part
    }
  }

  /*
  This method fixes the partition
   */
  def unpartition(all: HashSet[ActorRef]): Unit = {
    otherServers ++= all.diff(otherServers)
    otherServers -= self
  }
}