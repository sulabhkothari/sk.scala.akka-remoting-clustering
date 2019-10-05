package clustering

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberRemoved, MemberUp, UnreachableMember}
import akka.util.Timeout
import clustering.ChatDomain.{ChatMessage, EnterRoom, UserMessage}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

object ChatDomain {

  case class ChatMessage(nickname: String, contents: String)

  case class UserMessage(contents: String)

  case class EnterRoom(fullAddress: String, nickname: String)

}

class ChatActor(nickname: String, port: Int) extends Actor with ActorLogging {

  import scala.concurrent.duration._
  import context.dispatcher
  import akka.pattern.pipe

  val cluster = Cluster(context.system)
  implicit val timeout = Timeout(3 seconds)

  var chatRoomMembers: Map[String, ActorRef] = Map()

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  // TODO 1: Initialize the cluster object
  // TODO 2: Subscribe to cluster events in preStart()
  // TODO 3: Unsubscribe self in postStop()

  override def receive: Receive = online(Map())

  def getActorRef(clusterAddr: String) = {
    context.actorSelection(s"${clusterAddr}/user/chatActor")
  }

  def online(chatRoom: Map[String, String]): Receive = {
    case MemberUp(member) =>
      getActorRef(member.address.toString) ! EnterRoom(s"${self.path.address}@localhost:$port", nickname)

    case MemberRemoved(member, _) =>
      log.info(s"${chatRoom.getOrElse(member.address.toString, nickname)} left chat room")
      context.become(online(chatRoom - member.address.toString))

    case EnterRoom(fullAddress, remoteNickName) if remoteNickName != nickname =>
      log.info(s"Enter Room message from $remoteNickName")
      context.become(online(chatRoom + (fullAddress -> remoteNickName)))

    case umsg: UserMessage =>
      chatRoom.keys.foreach { addr =>
        getActorRef(addr) ! ChatMessage(nickname, umsg.contents)
      }

    case ChatMessage(remoteNickname, contents) =>
      log.info(s"$remoteNickname said $contents")
  }

  def receive2: Receive = {
    //    case ref: ActorRef =>
    //      log.info(s"Got actorref: $ref")
    //      ref ! EnterRoom(s"${cluster.selfAddress}/user/chatActor", nickname)
    case MemberUp(member) //if member.address != cluster.selfAddress
    =>
      // TODO 4: Send a special EnterRoom message to the chatActor deployed on the new node (hint: Use actorSelection)
      val actorSelected = context.actorSelection(s"${member.address}/user/chatActor")
      actorSelected.resolveOne() //.pipeTo(self)
        .foreach { ref =>
        ref ! EnterRoom(s"${cluster.selfAddress}/user/chatActor", nickname)
      }
    case MemberRemoved(member, _) =>
      // TODO 5: Remove the member from your data structures
      log.info(s"Remove $member")
      chatRoomMembers -= s"${member.address}/user/chatActor"
      log.info(s"$chatRoomMembers")

    case EnterRoom(fullAddress, remoteNickName) if remoteNickName != nickname =>
      // TODO 6: Add the member to your data structures
      log.info(s"Enter Room message from $nickname")
      context.actorSelection(fullAddress).resolveOne().foreach { ref =>
        chatRoomMembers += (fullAddress -> ref)
      }

    case umsg: UserMessage =>
      // TODO 7: Broadcast the content as chat messages to the rest of the cluster members
      chatRoomMembers.values.foreach { ref =>
        ref ! ChatMessage(nickname, umsg.contents)
      }

    case ChatMessage(remoteNickname, contents) =>
      log.info(s"$remoteNickname said $contents")
  }
}

class ChatApp(nickname: String, port: Int) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
   """.stripMargin)
    .withFallback(ConfigFactory.load("clustering/clusterChat.conf"))

  val system = ActorSystem("RTJVMCluster", config)
  val chatActor = system.actorOf(Props(new ChatActor(nickname, port)), "chatActor")

  scala.io.Source.stdin.getLines().foreach { line =>
    chatActor ! UserMessage(line)
  }
}

object Alice extends ChatApp("Alice", 2551)

object Bob extends ChatApp("Bob", 2552)

object Charlie extends ChatApp("Charlie", 2553)