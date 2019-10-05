package clustering

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props, ReceiveTimeout}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberJoined, MemberRemoved, MemberUp, UnreachableMember}
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import akka.util.Timeout
import clustering.ClusteringExampleDomain.{ProcessFile, ProcessLine, ProcessLineResult}
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Random

object ClusteringExampleDomain {

  case class ProcessFile(fileName: String)

  case class ProcessLine(line: String, aggregator: ActorRef)

  case class ProcessLineResult(count: Int)

}

class ClusterWordCountPriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedPriorityMailbox(
    PriorityGenerator {
      case m: MemberEvent => 0
      case _ => 1
    }
  )

class Master extends Actor with ActorLogging {

  import ClusteringExampleDomain._
  import context.dispatcher
  import scala.concurrent.duration._
  import akka.pattern.pipe

  implicit val timeout = Timeout(3 seconds)
  val cluster = Cluster(context.system)

  // One node has single actor
  var workers: Map[Address, ActorRef] = Map()
  var pendingRemoval: Map[Address, ActorRef] = Map()

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = handleClusterEvents.orElse(handleWorkerRegistration).orElse(handleJob)

  def handleClusterEvents: Receive = {
    case MemberUp(member) if member.hasRole("worker") =>
      log.info(s"Member is up: ${member.address}")
      if (pendingRemoval.contains(member.address))
        pendingRemoval -= member.address
      else {
        val workerSelection = context.actorSelection(s"${member.address}/user/worker")
        workerSelection.resolveOne().map(ref => (member.address, ref)).pipeTo(self)
      }

    case UnreachableMember(member) if member.hasRole("worker") =>
      log.info(s"Member ${member.address} detected unreachable")
      val workerOption = workers.get(member.address)
      workerOption.foreach { ref =>
        pendingRemoval += (member.address -> ref)
      }
    case MemberRemoved(member, previousStatus) =>
      log.info(s"Member ${member.address} removed after $previousStatus")
      workers -= member.address

    case m: MemberEvent =>
      log.info(s"Another member event I don't care about: $m")
  }

  def handleWorkerRegistration: Receive = {
    case pair: (Address, ActorRef) =>
      log.info(s"Registering worker: $pair")
      workers += pair
  }

  def handleJob: Receive = {
    case ProcessFile(fileName) =>
      val aggregator = context.actorOf(Props[Aggregator], "aggregator")
      scala.io.Source.fromFile(fileName).getLines().foreach { line =>
        self ! ProcessLine(line, aggregator)
      }

    case processLine: ProcessLine =>
      val workerIndex = Random.nextInt((workers -- pendingRemoval.keys).size)
      val worker: ActorRef = (workers -- pendingRemoval.keys).values.toSeq(workerIndex)
      worker ! processLine
      Thread.sleep(10)
  }
}

class Aggregator extends Actor with ActorLogging {

  import ClusteringExampleDomain._
  import scala.concurrent.duration._

  context.setReceiveTimeout(3 seconds)

  override def receive: Receive = online(0)

  def online(totalCount: Int): Receive = {
    case ProcessLineResult(count) =>
      context.become(online(totalCount + count))
    case ReceiveTimeout =>
      log.info(s"Total count = $totalCount")
      context.setReceiveTimeout(Duration.Undefined)
  }
}

class Worker extends Actor with ActorLogging {
  override def receive: Receive = {
    case ProcessLine(line, aggregator) =>
      log.info(s"Processing: $line")
      aggregator ! ProcessLineResult(line.split(" ").length)
  }
}

object SeedNodes extends App {
  def createNode(port: Int, role: String, props: Props, actorName: String) = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.cluster.roles = ["$role"]
         |akka.remote.artery.canonical.port = $port
       """.stripMargin)
      .withFallback(ConfigFactory.load("clustering/clusteringExample.conf"))

    val system = ActorSystem("RTJVMCluster", config)

    system.actorOf(props, actorName)
  }

  val master = createNode(2551, "master", Props[Master], "master")
  createNode(2552, "worker", Props[Worker], "worker")
  createNode(2553, "worker", Props[Worker], "worker")

  Thread.sleep(10000)

  master ! ProcessFile("src/main/resources/txt/lipsum.txt")
}

object AdditionalWorker extends App {
  SeedNodes.createNode(2554, "worker", Props[Worker], "worker")
}