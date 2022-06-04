import akka.actor.{Actor, ActorRef}
import scala.collection.mutable.HashSet

case class LogClient(message: String, server: ActorRef)
case class Reply(message: String)

class Client(var servers: HashSet[ActorRef]) extends Actor {

  def receive = {
    case LogClient(message: String, server: ActorRef) => log(message, server)
    case Reply(message: String) => reply(message)
  }

  def log(message: String, server: ActorRef): Unit = {
    server ! LogServer(message)
  }

  def reply(message: String) = {
    println("reply from " + sender().path.toString + ": " + message)
  }
}