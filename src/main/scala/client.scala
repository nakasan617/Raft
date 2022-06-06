import akka.actor.{Actor, ActorRef}

import scala.concurrent.duration._
import scala.collection.mutable.HashSet
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

case class LogClient(message: String, server: ActorRef)
case class Reply(message: String)
case class AskLog(server: ActorRef)

class Client(var servers: HashSet[ActorRef]) extends Actor {

  def receive = {
    case LogClient(message: String, server: ActorRef) => log(message, server)
    case Reply(message: String) => reply(message)
    case AskLog(server: ActorRef) => showLog(server)
  }

  def log(message: String, server: ActorRef): Unit = {
    server ! LogServer(message)
  }

  def showLog(server: ActorRef) = {
    try {
      implicit val timeout = Timeout(2.seconds)
      val future = ask(server, ShowLog).mapTo[Array[Log]]
      Await.result(future, timeout.duration)
      val ar = future.value.get.get
      println("showing the log of " + server.path.toString)
      printLogs(ar)
    } catch {
      case e: java.util.concurrent.TimeoutException => {
        println("timed out getting the log from " + server.path.toString)
      }
    }
  }

  def reply(message: String) = {
    println("reply from " + sender().path.toString + ": " + message)
  }

  private def printLogs(logs: Array[Log]): Unit = {
    for(x <- logs) {
      print(x)
      print(", ")
    }
    println()
  }
}