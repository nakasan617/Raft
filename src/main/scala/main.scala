
import akka.actor.{ActorSystem, Props}
//import com.typesafe.config.ConfigFactory

object main extends App {
  val system = ActorSystem("Servers")
  val node1 = system.actorOf(Props(classOf[Server], "leader"))
  val node2 = system.actorOf(Props(classOf[Server], "follower"))
  val node3 = system.actorOf(Props(classOf[Server], "follower"))
  //val mapActors = context.actorOf(RoundRobinPool(numberMappers).props(Props(classOf[MapActor], reduceActors)))
  node1 ! AddServer(node2)
  node1 ! AddServer(node3)
  node2 ! AddServer(node3)
  node2 ! AddServer(node1)
  node3 ! AddServer(node1)
  node3 ! AddServer(node2)
  node3 ! Start
  node2 ! Start
  node1 ! Start

  Thread.sleep(20000)
  System.out.println("system terminating")
  system.terminate()
}