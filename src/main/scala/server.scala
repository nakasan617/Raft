import akka.actor.{Actor, ActorRef, ActorSystem, Props, Cancellable}
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import akka.util.Timeout

case object Start
case object HeartBeat
case object CheckSelf
case object Vote
case object Die
case object Revive
case class AddServer(server: ActorRef)
case class AddServers(servers: HashSet[ActorRef])
case class VoteForMe(theirTerm: Int)
case class FollowMe(theirTerm: Int, theirRole: String)
case class LeaderNotify(theirTerm: Int)
case class Log(ref: ActorRef, message: String, Id: Int)
case class LogServer(message: String)
case class LogReplication(log: Log, theirTerm: Int)
case class AckPrecommit(log: Log)
case class Commit(log: Log)

class Server(var role: String) extends Actor {
  var leader: ActorRef = null
  var alive: Boolean = true
  val constantTime: Int = 500
  val otherServers: HashSet[ActorRef] = new HashSet()
  var cancellable: Cancellable = null
  val system = context.system
  val r = scala.util.Random
  var term: Int = 0
  var numVotes: Int = 0
  var currTerm: Int = 0
  var logs: List[Log] = List()
  var preCommit: HashMap[Log, Int] = new HashMap()
  var logId: Int = 0

  def receive = {
    case message: String => println(message)
    case Start =>
      if(role == "leader") sendHeartBeats()
      else waitForHeartBeat()
    case HeartBeat => if(alive) receiveHeartBeat()
    case AddServer(server: ActorRef) => if(alive) addServer(server)
    case AddServers(servers: HashSet[ActorRef]) => if(alive) addServers(servers)
    case VoteForMe(theirTerm: Int) => if(alive) voteForMe(theirTerm, sender())
    case CheckSelf => if(alive) sender() ! "OK"
    case Vote => if(alive) voteReceived()
    case FollowMe(theirTerm: Int, theirRole: String) => if(alive) followMe(theirTerm, theirRole)
    case LeaderNotify(theirTerm: Int) => if(alive) leaderNotify(theirTerm: Int)
    case Die => if(alive) die()
    case Revive => if(!alive) revive()
    case LogServer(message) => logServer(message)
    case LogReplication(log: Log, theirTerm: Int) => logReplication(log, theirTerm)
    case AckPrecommit(log: Log) => ackPrecommit(log)
    case Commit(log: Log) => commit(log)
  }

  /*
  this is the method for the leader
  it sends the heart beat to each followers
   */
  def sendHeartBeats() = {
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleWithFixedDelay(0.seconds, 200.millis) { () =>
      implicit val timeout = Timeout(5.seconds)
      otherServers.foreach((server: ActorRef) => server ! HeartBeat)
      val future = ask(self, CheckSelf)
      Await.result(future, 50.millis)
      if(future == null) {
        cancellable.cancel()
      }
    }
  }

  /*
  this method waits for the heart beat for each interval,
  if it doesn't get the heart beat,
  it starts the election
  */
  def waitForHeartBeat() = {
    val time = r.nextInt(300) + constantTime
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(time.millis) {
      implicit val timeout = Timeout(5.seconds)
      startElection()
    }
  }

  /*
  this is what we do when we receive the heart beat,
  we cancel the scheduled election and start a new schedule
   */
  def receiveHeartBeat() = {
    if(cancellable != null) {
//      println("cancelled at :" + self.path.toString)
      cancellable.cancel()
    } else {
      println("cancellable was null")
    }
//    println("received heartbeat: " + cnt)
//    cnt = cnt + 1
    val time = r.nextInt(300) + constantTime
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(time.millis) {
      implicit val timeout = Timeout(5.seconds)
      startElection()
    }
  }

  /*
  this methods adds other servers
   */
  def addServer(server: ActorRef) = {
    otherServers += server
  }

  /*
  this methods adds HashSet of servers
   */
  def addServers(servers: HashSet[ActorRef]) = {
    otherServers ++= servers
    otherServers -= self
  }

  /*
  this method is what the follower does when the leader dies and new node has to be elected
  numVotes starts out as one because you vote for yourself
   */
  def startElection() = {
    term += 1
    println("did not receive the heartBeat, starting leader election: " + self.path.toString + ", now at term " + term)
    role = "candidate"
    numVotes = 1
    otherServers.foreach((server: ActorRef) => server ! VoteForMe(term))
  }

  def voteForMe(theirTerm: Int, candidate: ActorRef) = {
    if(role == "follower") {
      if(theirTerm > term) {
        if(cancellable != null) {
          cancellable.cancel()
        }
        println("I am " + self.path.toString + ", voting for " + candidate.path.toString + " in term " + theirTerm)
        candidate ! Vote
        term = theirTerm
      } else if(theirTerm == term) {
        // you already voted for someone
      } else {
        // term > theirTerm
        // I will leave this part out yet
      }
    } else if (role == "leader") {
      if(theirTerm > term) {
        role = "follower"
        if(cancellable != null) {
          cancellable.cancel()
        }
        candidate ! Vote
      } else if(theirTerm < term) {
        candidate ! FollowMe(term, role)
      } else {
        println("The term is the same and you guys are candidate and leader, this can't be happening")
      }
    } else {
      assert(role == "candidate")
      if(theirTerm == term) {
        // don't do anything
      } else if (theirTerm > term) {
        // I need to follow them
        role = "follower"
        term = theirTerm
        println("term changed at " + self.path.toString() + " at vote for me")
        candidate ! Vote
      } else {
        // they need to follow me
        assert(theirTerm < term)
        candidate ! FollowMe(term, role)
      }
    }
  }

  def followMe(theirTerm: Int, theirRole: String): Unit = {
    assert(theirTerm > term)
    if(theirRole == "leader") {
      term = theirTerm
      role = "follower"
      receiveHeartBeat()
    } else if(theirRole == "candidate") {
      term = theirTerm
      role = "follower"
      sender() ! Vote
    } else {
      println("we are at follow me and ")
    }
  }

  def voteReceived(): Unit = {
    if(term > currTerm) {
      numVotes += 1
      val half: Int = otherServers.size / 2
      if (numVotes > half) {
        println("leader elected, " + self.path.toString + " is the new leader at term " + term)
        currTerm = term
        otherServers.foreach((server: ActorRef) => {
//          println("sending to " + server.path.toString)
          implicit val timeout = Timeout(1.seconds)
          try {
            val future = ask(server, LeaderNotify(term)).mapTo[String]
            Await.result(future, timeout.duration)
            if (future == null || future.value == null) {
              println("leader notify did not work")
              system.terminate() // this is basically asserting
            } else if (future.value.get.get == "No Good") {
              println("there is a problem with the term")
              system.terminate() // this is basically asserting for now
            } else if (future.value.get.get == "OK") {
//              println("OK was received")

            } else {
              println("some random message received")
            }
          } catch {
            case te: java.util.concurrent.TimeoutException => {
              println("timeout exception caught when sending it to " + server.path.toString)
            }
          }
        })
        role = "leader"
        sendHeartBeats()
      }
    }
  }

  def leaderNotify(theirTerm: Int): Unit = {
    if(theirTerm >= term) {
      sender() ! "OK"
      term = theirTerm
      role = "follower"
      leader = sender()
    } else {
      sender() ! "No Good"
    }
  }

  def die(): Unit = {
    alive = false
    cancellable.cancel()
    cancellable = null
  }

  def revive(): Unit = {
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
        leader ! LogServer(message)
      } else {
        println("leader does not exist, aborting the message: " + message)
      }
    } else {
      // add the entry
      val log = Log(self, message, logId)
      preCommit(log) = 0
      logs = log::logs
      logId = logId + 1
      otherServers.foreach((server: ActorRef) => server ! LogReplication(log, term))
    }
  }

  /*
  This method is used when the follower receives the precommit message as well as its logs
  The term has to be the same to do the replication and if the term is not right, it is ignored
   */
  def logReplication(log: Log, theirTerm: Int): Unit = {
    if(theirTerm == term) {
      preCommit(log) = 0
      sender() ! AckPrecommit
    } else {
      // just ignore in this case
    }
  }

  /*
  This method is used by the leader when the follower sends the acknowledgement for the precommit
  You will store the number of acknowledge in the HashMap, and when it is over half, it is removed from the hashmap, added to the log,
  and sends messages to the followers to commit as well
   */
  def ackPrecommit(log: Log): Unit = {
    assert(role == "leader")
    preCommit(log) += 1
    if((otherServers.size + 1)/2  < preCommit(log) && preCommit(log) <= (otherServers.size + 1)/2 + 1) {
      preCommit -= (log)
      otherServers.foreach((server: ActorRef) => server ! Commit(log))
    }
  }

  def commit(log: Log) = {
    assert(role == "follower")
    if(preCommit.contains(log)) {
      logs = log::logs
    } else {
      println("commit message for log that is not in precommit: " + log.message + ", " + log.ref.path.toString + ", " + log.Id)
    }
  }
}