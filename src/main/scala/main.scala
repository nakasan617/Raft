
import akka.actor.{ActorSystem, Props, PoisonPill}
//import com.typesafe.config.ConfigFactory

object main extends App {
  val system = ActorSystem("Servers")
  val node1 = system.actorOf(Props(classOf[Server], "leader"))
  val node2 = system.actorOf(Props(classOf[Server], "follower"))
  val node3 = system.actorOf(Props(classOf[Server], "follower"))
  val node4 = system.actorOf(Props(classOf[Server], "follower"))
  val node5 = system.actorOf(Props(classOf[Server], "follower"))
  //val mapActors = context.actorOf(RoundRobinPool(numberMappers).props(Props(classOf[MapActor], reduceActors)))
  node1 ! AddServer(node2)
  node1 ! AddServer(node3)
  node1 ! AddServer(node4)
  node1 ! AddServer(node5)
  node2 ! AddServer(node3)
  node2 ! AddServer(node4)
  node2 ! AddServer(node5)
  node2 ! AddServer(node1)
  node3 ! AddServer(node4)
  node3 ! AddServer(node5)
  node3 ! AddServer(node1)
  node3 ! AddServer(node2)
  node4 ! AddServer(node5)
  node4 ! AddServer(node1)
  node4 ! AddServer(node2)
  node4 ! AddServer(node3)
  node5 ! AddServer(node1)
  node5 ! AddServer(node2)
  node5 ! AddServer(node3)
  node5 ! AddServer(node4)

  node5 ! Start
  node4 ! Start
  node3 ! Start
  node2 ! Start
  node1 ! Start

  Thread.sleep(2000)
  println("poisonPill to node1")
  node1 ! PoisonPill
  node1 ! "message"

  Thread.sleep(10000)
  println("system terminating")
  system.terminate()
}