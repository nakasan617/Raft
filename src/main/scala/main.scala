
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

  Thread.sleep(2000)
  /*
  println("poisonPill to node1")
  node1 ! PoisonPill
  node1 ! "message"
  */
  node1 ! Die

  Thread.sleep(10000)
  println("system terminating")
  system.terminate()
}