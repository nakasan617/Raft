import akka.actor.{Actor, ActorRef, ActorSystem, Props, Cancellable}

import scala.concurrent.duration._
import scala.collection.mutable.HashSet
import akka.util.Timeout

case object Start
case object HeartBeat
case object VoteForMe
case class AddServer(server: ActorRef)

class Server(var role: String) extends Actor {
  var cnt = 0
  val otherServers: HashSet[ActorRef] = new HashSet()
  var cancellable: Cancellable = null
  val system = context.system
  var term: Int = 0

  def receive = {
    case Start =>
      if(role == "leader") sendHeartBeats()
      else waitForHeartBeat()
    case HeartBeat => receiveHeartBeat()
    case AddServer(server: ActorRef) => addServer(server)
    case VoteForMe => println("vote for me received from :" + sender.path.toString)
  }

  /*
  this is the method for the leader
  it sends the heart beat to each followers
   */
  def sendHeartBeats() = {
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleWithFixedDelay(0.seconds, 300.millis) { () =>
      implicit val timeout = Timeout(5.seconds)
      otherServers.foreach((server: ActorRef) => server ! HeartBeat)
    }
  }

  /*
  this method waits for the heart beat for each interval,
  if it doesn't get the heart beat,
  it starts the election
  */
  def waitForHeartBeat() = {
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(2.seconds) {
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
      cancellable.cancel()
    } else {
      println("cancellable was null")
    }
//    println("received heartbeat: " + cnt)
    cnt = cnt + 1
    implicit val ec = system.dispatcher
    cancellable = system.scheduler.scheduleOnce(2.seconds) {
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

  def startElection() = {
    println("did not receive the heartBeat, starting leader election: " + self.path.toString)
    term += 1
    role = "candidate"
    otherServers.foreach((server: ActorRef) => server ! VoteForMe)
  }
}