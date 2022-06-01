
import akka.actor.{ActorSystem, Props}
//import com.typesafe.config.ConfigFactory

object main extends App {
  val system = ActorSystem("Servers")
  val node1 = system.actorOf(Props[Server](), name = "node1")
  val node2 = system.actorOf(Props[Server](), name = "node2")
  val node3 = system.actorOf(Props[Server](), name = "node3")
  node1 ! AddServer(node2)
  node1 ! AddServer(node3)
  node1 ! Start
  Thread.sleep(20000)
  System.out.println("system terminating")
  system.terminate()
}