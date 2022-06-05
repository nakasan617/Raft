
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.util.Timeout

import scala.collection.mutable.HashSet
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask

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
  var leader: ActorRef = node1
  try {
    implicit val timeout = Timeout(1.seconds)
    val future = ask(node1, AskLeader).mapTo[ActorRef]
    Await.result(future, timeout.duration)
//    println(future.value.get.get)
    leader = future.value.get.get
  } catch {
    case e: java.util.concurrent.TimeoutException => {
      println("timed out getting the log from " + leader.path.toString)
      Thread.sleep(2000)
      system.terminate()
    }
  }
  println(leader.path.toString)
  Thread.sleep(1000)
  leader ! Die
  Thread.sleep(2000)

  client ! LogClient("I hope", node5)
  client ! LogClient("This final project", node3)
  client ! LogClient("Is good", node1)
  client ! LogClient("hello world", node4)

  client ! AskLog(node3)
  Thread.sleep(1000)
  client ! LogClient("Is good", node2)
  client ! AskLog(node4)
  client ! AskLog(node3)

  leader ! Revive

  Thread.sleep(5000)
  client ! AskLog(leader)
  client ! AskLog(node2)

  Thread.sleep(5000)
  println("system terminating")
  system.terminate()
}