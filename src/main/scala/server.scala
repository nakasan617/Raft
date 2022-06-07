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
case class CatchUpLeft(log: Log, theirIndex: Int)
case class CatchUpRight()
case class CatchUpReply(log: Log, Index: Int, message: String)
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
  var index: Int = -1
  var indexes: HashMap[ActorRef, Int] = new HashMap()
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
    case CatchUpLeft(log: Log, theirIndex: Int) => if(alive) catchUpLeft(log, theirIndex)
    case CatchUpRight() => if(alive) catchUpRight()
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
        catchUp(theirTerm)
      }
    } else if(term < theirTerm) {
      // I am late, so you need to catch up with the leader (sender is the leader here)
      catchUp(theirTerm)
    } else {
      // you just ignore if the leader is late (the older leader will get the heart beat from the new leader eventually)
    }
  }

  private def catchUp(theirTerm: Int): Unit = {
    val oldTerm = term
    role = "follower"
    term = theirTerm
    var caughtUp = false
    var left = true
    var currIndex: Int = logs.size - 1

    implicit val timeout = Timeout(1.seconds)

    /*
    print("we are follower here: ")
    for(x <- logs) {
      print(x)
      print(", ")
    }
    println()
     */

    while(!caughtUp) {
      var future:Future[CatchUpReply] = null
      try {
        if(left) {
          if(currIndex == -1) {
            future = ask(sender(), CatchUpLeft(Log(self, "NOLOG", -1), currIndex)).mapTo[CatchUpReply]
          } else {
            future = ask(sender(), CatchUpLeft(logs(currIndex), currIndex)).mapTo[CatchUpReply]
          }
        } else {
          future = ask(sender(), CatchUpRight()).mapTo[CatchUpReply]
        }
        Await.result(future, timeout.duration)
        //          println(future)
      } catch {
        case e: TimeoutException => {
          term = oldTerm
          println("timed out exception, we need to redo this")
          caughtUp = true // break from the loop and hope for the next opportunity
        }
      }
      val reply: CatchUpReply = future.value.get.get
      if(reply.message == "Index") {
//        println("index has to be aligned, now it is " + reply.Index)
        currIndex = reply.Index
      } else if(reply.message == "Left") {
//        println("the reply said keep going left, currently it is at: " + reply.Index + "," + logs(reply.Index))
        currIndex = reply.Index - 1
      } else if(reply.message == "Right") {
        if(reply.Index < logs.size) {
//          println("the reply now says you can start going right, currIndex: " + reply.Index + ", logs(currIndex): " + logs(reply.Index))
          logs(reply.Index) = reply.log
        } else if(reply.Index == logs.size) {
//          println("the reply now says you can start going right, currIndex: " + reply.Index)
          logs = logs :+ reply.log
        } else {
          println("this was not expected. terminating")
          system.terminate()
        }
        currIndex = reply.Index
        left = false
      } else if(reply.message == "Done") {
        lastCommittedLog = logs(logs.size - 1)
        caughtUp = true
      } else {
        println("we need to redo this")
        println(reply.message)
        caughtUp = true
        term = oldTerm // you need to redo the catching up again
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
    println("did not receive the heartBeat, starting leader election: " + self.path.toString + ", now at term " + term + "at reelectionIndex: " + reelectionIndex)
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

  private def restartElection(): Unit = {
    reelectionIndex += 1
    numVotes = 1
    role = "candidate"
    println("could not elect the leader, will restart: " + self.path.toString + " at term " + term + " at reelectionindex: " + reelectionIndex)
    scheduleRestartElection()
    otherServers.foreach((server: ActorRef) => server ! VoteForMe(term, reelectionIndex))
  }

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
//    println(self.path.toString + ", role: " + role + ", term: " + term + ", RI: " + reelectionIndex)
    if(role == "follower") {
      if(theirTerm > term) {
        if(cancellable != null) {
          cancellable.cancel()
        }

//        println("I am " + self.path.toString + ", voting for " + candidate.path.toString + " in term " + theirTerm)
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
//             println("OK was received")
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
        println("forwarding the message: " + message + " to " + leader.path.toString)
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
      //println("otherServers: " + otherServers)
      otherServers.foreach((server: ActorRef) => server ! LogReplication(log, term))
    }
  }

  /*
  This method is used when the follower receives the precommit message as well as its logs
  The term has to be the same to do the replication and if the term is not right, it is ignored
   */
  def logReplication(log: Log, theirTerm: Int): Unit = {
    if(role != "follower") {
      println("I am at logReplication, role: " + role + ", " + self.path.toString + ", theirTerm: " + theirTerm + ", myTerm: " + term)
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
//      println("precommit for " + log.message + " by " + sender().path.toString)
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
//      println("commit message for log that is not in precommit: " + log.message + ", " + log.ref.path.toString + ", " + log.Id)
    }
  }

  /*
  This method is used when the former node revives and want to catch up with the current logs BY the leader
  It first tries to see till where the logs are valid comparing with the current leader
  When it finds the latest point where the logs do NOT need to be updated, it hands over the procedure to catchUpRight
   */
  def catchUpLeft(log: Log, theirIndex: Int): Unit = {
    assert(role == "leader")
    println("at catchUpLeft, index: " + index + ", theirIndex: " + theirIndex + " self: " + self.path.toString)
    try {
      if(log.message == "NOLOG") {
        sender() ! CatchUpReply(logs(0), 0, "Right")
        index = 1
      } else {
        if (index == -1) {
          index = logs.size - 1
        }

        /*
      print("we are leader here, logs: ")
      for(x <- logs) {
        print(x)
        print(", ")
      }
      println()
       */

        if (index < theirIndex) {
          println("the index needs to be aligned, myIndex: " + index + " theirIndex: " + theirIndex)
          sender() ! CatchUpReply(null, index, "Index")
        } else if (theirIndex < index) {
          println("the index needs to be aligned, but I am ahead, so I can go back to theirIndex: " + theirIndex)
          index = theirIndex
          if (logs(index) == log) {
            println("the log was the same, you can start going right now: " + index)
            sender() ! CatchUpReply(logs(index + 1), index + 1, "Right")
          } else {
            println("the log was not the same")
            if (index == 0) {
              println("but now we are at 0, so let's start going right now")
              sender() ! CatchUpReply(logs(index), index, "Right")
            } else {
              println("its not the same, so we should keep going left, index: " + index)
              sender() ! CatchUpReply(null, index - 1, "Left")
              index -= 1
            }
          }
        } else { // theirIndex == index
          println("the index is already aligned")
          if (logs(index) == log) {
            println("the log is the same, let's start going right now: " + index)
            if (index + 1 < logs.size) {
              sender() ! CatchUpReply(logs(index + 1), index + 1, "Right")
              index += 1
            } else {
              // this is already done
              sender() ! CatchUpReply(null, index, "Done")
            }
          } else {
            println("the log is not the same, let's keep going left, index: " + index)
            sender() ! CatchUpReply(null, index - 1, "Left")
            index -= 1
          }
        }
      }
    } catch {
      case e: java.lang.ArrayIndexOutOfBoundsException => {
        println("index out of bounds, index: " + index + ", logs: " + logs)
        println("this cannot be recovered, system terminating")
        system.terminate()
      }
    }
  }

  /*
  This method is used by leader, when the leader and the late follower knows where the latest point which follower does not need to update,
  the current leader will keep on giving out a log at a time to the late follower
   */
  def catchUpRight(): Unit = {
    try {
      if (index < logs.size) {
        println("we will keep giving out the logs as well as index, index: " + index + ", logs(index): " + logs(index))
        sender() ! CatchUpReply(logs(index), index, "Right")
        index += 1
      } else {
        println("we should be done catching up now")
        sender() ! CatchUpReply(null, index, "Done")
        index = -1
      }
    } catch {
      case e: java.lang.ArrayIndexOutOfBoundsException => {
        println("index out of bounds at " + self.path.toString + ", index: " + index + " logs: ")
        printLogs()
        println("this is not recoverable, system terminating")
        system.terminate()
      }
    }

  }

  def askLeader()= {
    sender() ! leader
  }

  private def printLogs(): Unit = {
    for(x <- logs) {
      print(x)
      print(", ")
    }
    println()
  }

  def partition(part: HashSet[ActorRef]): Unit = {
    if(part.contains(self)) {
      val res = otherServers.diff(part)
      otherServers --= res
    } else {
      otherServers --= part
    }
  }

  def unpartition(all: HashSet[ActorRef]): Unit = {
    otherServers ++= all.diff(otherServers)
    otherServers -= self
  }
}