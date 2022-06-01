import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.concurrent.duration._
import scala.collection.mutable.HashSet
import akka.util.Timeout

case object Start
case object HeartBeat
case class AddServer(server: ActorRef)

class Server extends Actor {
  var cnt = 0
  val otherServers: HashSet[ActorRef] = new HashSet()

  def receive = {
    case Start => sendHeartBeats()
    case HeartBeat => receiveHeartBeat()
    case AddServer(server: ActorRef) => addServer(server)
  }

  def sendHeartBeats() = {
    val system = context.system
    implicit val ec = system.dispatcher
    system.scheduler.scheduleWithFixedDelay(0.seconds, 300.millis) { () =>
      implicit val timeout = Timeout(5.seconds)
      otherServers.foreach((server: ActorRef) => server ! HeartBeat)
    }
  }

  def receiveHeartBeat() = {
    println("received heartbeat: " + cnt)
    cnt = cnt + 1
  }

  def addServer(server: ActorRef) = {
    otherServers += server
  }
}