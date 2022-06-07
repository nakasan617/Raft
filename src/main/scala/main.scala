
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout

import scala.collection.mutable.HashSet
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask

/*
This is a test that combines leader election, log committing, and log replication
 */
object MainTest extends App {
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
  var leader: ActorRef = node1
  try {
    implicit val timeout = Timeout(1.seconds)
    val future = ask(node1, AskLeader).mapTo[ActorRef]
    Await.result(future, timeout.duration)
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

/*
This is a test to see if it can create a new leader when it becomes absent.
It first creates 5 nodes and then start, which would start the leader election.
Then it kills the leader and let's another voting start again.
The leader that is different from the former should newly be elected.
 */
object LeaderElectionTest extends App {
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

  Thread.sleep(6000)
  println("Finding the leader of this term")
  var leader: ActorRef = node1
  try {
    implicit val timeout = Timeout(1.seconds)
    val future = ask(node1, AskLeader).mapTo[ActorRef]
    Await.result(future, timeout.duration)
    leader = future.value.get.get
  } catch {
    case e: java.util.concurrent.TimeoutException => {
      println("timed out getting the log from " + leader.path.toString)
      Thread.sleep(2000)
      system.terminate()
    }
  }
  println("leader: " + leader.path.toString + " I am killing this guy for a moment")
  Thread.sleep(1000)
  leader ! Die
  Thread.sleep(5000)
  println("Did it elect the new leader?")
  println("system terminating")
  system.terminate()
}

/*
This is a test that creates some logs for servers and see if they have the same logs
 */
object LogCommitTest extends App {
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

  Thread.sleep(1000)
  val client = system.actorOf(Props(classOf[Client], nodes))
  println("Adding many logs to the system now")
  client ! LogClient("I hope", node5)
  client ! LogClient("This final project", node3)
  client ! LogClient("Is good", node1)
  client ! LogClient("hello world", node4)

  Thread.sleep(3000)
  println("Asking for the logs in some nodes")
  client ! AskLog(node1)
  client ! AskLog(node3)
  client ! AskLog(node5)

  Thread.sleep(2000)
  println("Were the logs same?")
  system.terminate()
}

/*
This is a test that creates the logs first, kills the leader, creates more logs, and revives the leader.
It checks whether the former leader's log is consistent with others' logs
 */
object ReviveTest extends App {
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

  Thread.sleep(1000)
  val client = system.actorOf(Props(classOf[Client], nodes))
  println("Adding many logs to the system now")
  client ! LogClient("I hope", node5)
  client ! LogClient("This final project", node3)
  Thread.sleep(1500)
  println("Below is the log before the leader dying")
  client ! AskLog(node2)

  println("Finding the leader of this term")
  var leader: ActorRef = node1
  try {
    implicit val timeout = Timeout(1.seconds)
    val future = ask(node1, AskLeader).mapTo[ActorRef]
    Await.result(future, timeout.duration)
    leader = future.value.get.get
  } catch {
    case e: java.util.concurrent.TimeoutException => {
      println("timed out getting the log from " + leader.path.toString)
      Thread.sleep(2000)
      system.terminate()
    }
  }
  println("leader: " + leader.path.toString + " I am killing this guy for a moment")
  Thread.sleep(1000)
  leader ! Die
  Thread.sleep(2000)

  client ! LogClient("Chicago is cold", node1)
  client ! LogClient("I want money", node2)
  client ! LogClient("hello world", node3)
  client ! LogClient("I want time", node4)
  client ! LogClient("Life is good", node5)

  Thread.sleep(2000)
  client ! AskLog(node2)
  leader ! Revive
  println("asking log right after reviving")
  client ! AskLog(leader)

  Thread.sleep(2000)
  println("asking log some time after reviving")
  client ! AskLog(leader)

  Thread.sleep(5000)
  system.terminate()
}

/*
This is a test to see if the candidate will keep on holding the election if the more than half the nodes are dead.
 */
object RecurringLeaderElection extends App {
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

  Thread.sleep(1000)
  println("Finding the leader of this term")
  var leader: ActorRef = node1
  try {
    implicit val timeout = Timeout(1.seconds)
    val future = ask(node1, AskLeader).mapTo[ActorRef]
    Await.result(future, timeout.duration)
    leader = future.value.get.get
  } catch {
    case e: java.util.concurrent.TimeoutException => {
      println("timed out getting the log from " + leader.path.toString)
      Thread.sleep(2000)
      system.terminate()
    }
  }
  println("leader: " + leader.path.toString + " I am killing this guy for a moment")
  Thread.sleep(1000)
  var leaderDied: Boolean = false
  if(node1 == leader || node2 == leader) {
    leaderDied = true
  }
  node1 ! Die
  node2 ! Die
  if(leaderDied == true) {
    node3 ! Die
  } else {
    leader ! Die
  }
  Thread.sleep(10000)
  println("Did it elect the new leader? It shouldn't.")
  println("system terminating")
  system.terminate()
}

object PartitionTest extends App {
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

  val partition:HashSet[ActorRef] = new HashSet()
  partition += node1
  partition += node2

  Thread.sleep(2000)
  println("creating partition for " + node1.path.toString + " and " + node2.path.toString)
  nodes.foreach((node: ActorRef) => node ! Partition(partition))
  Thread.sleep(2000)

  val client = system.actorOf(Props(classOf[Client], nodes))
  println("Adding 2 logs to the system now")
  client ! LogClient("I hope", node5)
  client ! LogClient("This works", node4)

  Thread.sleep(5000)
  println("Unpartitioning")
  nodes.foreach((node: ActorRef) => node ! Unpartition(nodes))

  Thread.sleep(10000)
  println("system terminating")
  system.terminate()
}