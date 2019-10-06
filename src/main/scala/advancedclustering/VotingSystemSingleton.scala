package advancedclustering

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props, ReceiveTimeout}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory

import scala.util.Random

case class Person(id: String, age: Int)

object Person {
  def generate() = Person(UUID.randomUUID().toString, 16 + Random.nextInt(90))
}

case class Vote(person: Person, candidate: String)

case object VoteAccepted

case class VoteRejected(reason: String)

class VotingAggregator extends Actor with ActorLogging {
  val candidates = Set("Martin", "Roland", "Jonas", "Daniel")

  import scala.concurrent.duration._

  context.setReceiveTimeout(30 seconds)

  override def receive: Receive = online(Set(), Map())

  def offline: Receive = {
    case v: Vote =>
      log.warning(s"Received vote $v which is invalid as the time is up")
      sender() ! VoteRejected("cannot accept vote after poll closing time")
    case m =>
      log.warning(s"Received $m - will not process more messages after polls closing time")
  }

  def online(personsVoted: Set[String], polls: Map[String, Int]): Receive = {
    case Vote(Person(id, age), candidate) =>
      if (personsVoted.contains(id)) sender ! VoteRejected("Already Voted")
      else if (age < 18) sender ! VoteRejected("Not above legal voting age")
      else if (!candidates.contains(candidate)) sender() ! VoteRejected("Invalid candidate")
      else {
        log.info(s"Recording vote from person $id for $candidate")
        val candidateVotes = polls.getOrElse(candidate, 0)
        sender() ! VoteAccepted
        context.become(online(personsVoted + id, polls + (candidate -> (candidateVotes + 1))))
      }

    case ReceiveTimeout =>
      log.info(s"TIME's up, here are the poll results: $polls")
      context.setReceiveTimeout(Duration.Undefined)
      context.become(offline)
  }
}

object VotingStation {
  def props(votingAggregator: ActorRef) = Props(new VotingStation(votingAggregator))
}

class VotingStation(votingAggregator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case v: Vote =>
      votingAggregator ! v
    case VoteAccepted =>
      log.info("Vote was accepted")
    case VoteRejected(reason) =>
      log.warning(s"Vote rejected: $reason")
  }
}

object CentralElectionSystem extends App {
  def startNode(port: Int) = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.remote.artery.canonical.port = $port
       """.stripMargin)
      .withFallback(ConfigFactory.load("advancedclustering/votingSystemSingleton.conf"))

    val system = ActorSystem("RTJVMCluster", config)
    // TODO 1  setup the cluster singleton

    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props[VotingAggregator],
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)
      ), "singletonVotingAggregator")
  }

  (2551 to 2553).foreach(startNode)
}

class VotingStationApp(port: Int) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
       """.stripMargin)
    .withFallback(ConfigFactory.load("advancedclustering/votingSystemSingleton.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  //TODO 2: setup communication to the cluster singleton
  val proxy = system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "user/singletonVotingAggregator",
      settings = ClusterSingletonProxySettings(system)
    )
  )

  val votingStation = system.actorOf(VotingStation.props(proxy), "votingStation")

  //TODO 3: read the line from stdin
  scala.io.Source.stdin.getLines().foreach { candidate =>
    votingStation ! Vote(Person.generate(), candidate)
  }
}

object Washington extends VotingStationApp(2561)
object NewYork extends VotingStationApp(2562)
object SanFrancisco extends VotingStationApp(2563)

/*
  If cluster singleton is a stateful actor and one of the nodes dies, then state will be lost.
  To address the issue, this actor needs to be persistent using some store like cassandra.
 */