
import akka.actor.{ActorSystem, Props, PoisonPill, ActorRef}
import scala.collection.mutable.HashSet

object main extends App {
  val system = ActorSystem("Raft")
  val node1 = system.actorOf(Props(classOf[Server], "follower"))
  val node2 = system.actorOf(Props(classOf[Server], "follower"))
  val node3 = system.actorOf(Props(classOf[Server], "follower"))
  val node4 = system.actorOf(Props(classOf[Server], "follower"))
  val node5 = system.actorOf(Props(classOf[Server], "follower"))

  val nodes: HashSet[ActorRef] = new HashSet()
  nodes += node1
  nodes += node2
  nodes += node3
  nodes += node4
  nodes += node5

  nodes.foreach((node: ActorRef) => node ! AddServers(nodes))
  nodes.foreach((node: ActorRef) => node ! Start)

  Thread.sleep(3000)
  val client = system.actorOf(Props(classOf[Client], nodes))
  client ! LogClient("Chicago is cold", node1)
  client ! LogClient("I want money", node2)
  client ! LogClient("hello world", node3)
  client ! LogClient("I want time", node4)
  client ! LogClient("Life is good", node5)

  Thread.sleep(5000)
  client ! AskLog(node2)
  client ! AskLog(node3)
  client ! AskLog(node1)
  /*
  println("poisonPill to node1")
  node1 ! PoisonPill
  node1 ! "message"
  */
  node1 ! Die

  client ! LogClient("I hope", node5)
  client ! LogClient("This final project", node3)
  client ! LogClient("Is good", node1)
  client ! LogClient("hello world", node4)

  client ! AskLog(node3)
  Thread.sleep(1000)
  client ! LogClient("Is good", node2)
  client ! AskLog(node4)
  client ! AskLog(node3)

  Thread.sleep(5000)
  println("system terminating")
  system.terminate()
}