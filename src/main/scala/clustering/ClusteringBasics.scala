package clustering

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberJoined, MemberRemoved, MemberUp, UnreachableMember}
import com.typesafe.config.ConfigFactory

class ClusterSubscriber extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  /*
    InitialStateAsEvents: for getting all events of nodes joining and leaving
    InitialStateAsSnapshot: if you just want the state of the cluster at the moment of Join
   */
  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case MemberJoined(member) =>
      log.info(s"New member joined ${member.address}")
    case MemberUp(member) if(member.hasRole("numberCruncher")) =>
      log.info(s"HELLO BROTHER: ${member.address}")
    case MemberUp(member) =>
      log.info(s"Let's say Welcome to newest member ${member.address}")
    case MemberRemoved(member, previousStatus) =>
      log.info(s"Poor ${member.address} was removed from $previousStatus")
    case UnreachableMember(member) =>
      log.info(s"Uh oh, member ${member.address} is unreachable")
    case m: MemberEvent =>
      log.info(s"Another member event $m")
  }
}

object ClusteringBasics extends App {
  def startCluster(ports: List[Int]): Unit =
    ports.foreach { port =>
      val config = ConfigFactory.parseString(
        s"""
           |akka.remote.artery.canonical.port = $port
         """.stripMargin
      ).withFallback(ConfigFactory.load("clustering/clusteringBasics.conf"))

      val system = ActorSystem("RTJVMCluster", config) // all the actor systems in a cluster must have the same name
      system.actorOf(Props[ClusterSubscriber], "clusterSubscriber")

      Thread.sleep(2000)
    }

  /*
    Order of seed nodes matters
    We get below log lines if order of the seed nodes is different in below startCluster method & in the config file
    "Couldn't join seed nodes after [2] attempts, will try again. seed-nodes=[akka://RTJVMCluster@localhost:2551]"
   */
  // 0 means the system will allocate a port for you e.g., 56284
  startCluster(List(2552, 2551, 0))
  /*
    This is where cluster starts:
      Node [akka://RTJVMCluster@localhost:2551] is JOINING itself (with roles [dc-default]) and forming new cluster
   */
}

object ClusteringBasics_ManualRegistration extends App {
  val system = ActorSystem("RTJVMCluster",
    ConfigFactory.load("clustering/clusteringBasics.conf").getConfig("manualRegistration"))
  val cluster = Cluster(system)

  def joinExistingCluster = cluster.joinSeedNodes(List(
    Address("akka", "RTJVMCluster", "localhost", 2551), // equivalent to AddressFromURIString
    Address("akka", "RTJVMCluster", "localhost", 2552)
  ))

  def joinExistingNode = cluster.join(Address("akka", "RTJVMCluster", "localhost", 63472))

  /*
    Will show below messages because it did not connect to any existing node in the cluster
    No seed-nodes configured
    is the new leader among reachable nodes (more leaders may exist)
   */
  def joinMyself = cluster.join(Address("akka", "RTJVMCluster", "localhost", 2555))

  joinExistingCluster
  system.actorOf(Props[ClusterSubscriber], "clusterSubscriber")
}
