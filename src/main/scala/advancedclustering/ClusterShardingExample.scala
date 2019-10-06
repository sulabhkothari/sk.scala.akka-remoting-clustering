package advancedclustering

import java.util.{Date, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, ReceiveTimeout}
import akka.cluster.sharding.ShardRegion.Passivate
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory

import scala.util.Random

case class OysterCard(id: String, amount: Double)

case class EntryAttempt(oysterCard: OysterCard, date: Date)

case object EntryAccepted

case class EntryRejected(reason: String)

// passivate message
case object TerminateValidator

//////////////////////////////////////////////////////////////////
//////////////////////       ACTORS         //////////////////////
//////////////////////////////////////////////////////////////////
object Turnstile {
  def props(validator: ActorRef) = Props(new Turnstile(validator))
}

class Turnstile(validator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case o: OysterCard => validator ! EntryAttempt(o, new Date)
    case EntryAccepted => log.info("GREEN: please pass")
    case EntryRejected(reason) => log.info(s"RED: $reason")

  }
}

class OysterCardValidator extends Actor with ActorLogging {
  /*
    Stores an enormous amount of data which warrants the need of sharding
   */

  import scala.concurrent.duration._

  override def preStart(): Unit = {
    super.preStart()
    log.info("Validator starting")
    context.setReceiveTimeout(10 seconds)
  }

  override def receive: Receive = {
    case EntryAttempt(card@OysterCard(id, amount), _) =>
      log.info(s"Validating card $card")
      if (amount > 2.5) sender ! EntryAccepted
      else sender() ! EntryRejected(s"[$id] not enough funds, please top up")
    case ReceiveTimeout =>
      context.parent ! Passivate(TerminateValidator)
    case TerminateValidator =>
      // I am sure that I won't be contacted again, so safe to stop
      context.stop(self)
  }
}

//////////////////////////////////////////////////////////////////
//////////////////////    Sharding Settings     //////////////////
//////////////////////////////////////////////////////////////////

object TurnstileSettings {
  val numberOfShards = 10 // use 10x the number of nodes in your cluster
  val numberOfEntities = 100 // 10x the number of shards

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case attempt@EntryAttempt(OysterCard(cardId, _), _) =>
      val entityId = cardId.hashCode.abs % numberOfEntities
      (entityId.toString, attempt)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntryAttempt(OysterCard(cardId, _), _) =>
      val shardId = cardId.hashCode.abs % numberOfShards
      shardId.toString
    case ShardRegion.StartEntity(entityId) =>
      /*
       Message -> extractEntityId 43
               -> extractShardId 9

       There must be NO two messages M1 and M2 for which
       extractEntityId(M1) == extractEntity(M2) and extractShardId(M1) == extractShardId(M2)

       M1 -> E37, S9
       M2 -> E37, S10
       This means these two shards if they operate on different nodes, will create this E37 twice in the cluster.
       This is especially bad if your E37 is also a persistent actor (will corrupt the journals because of the same persistentId)

       entityId -> shardId, then FORALL messages M, if extractEntityId(M) = entityId, then extractShardId(M) MUST BE shardId
      */

      // It works here because numberOfShards is a divisor of numberOfEntities
      (entityId.toLong % numberOfShards).toString

      // Second step is: ClusterShardingSettings(system).withRememberEntities(true), as done in below code
  }
}


//////////////////////////////////////////////////////////////////
//////////////////////    Cluster Nodes     //////////////////////
//////////////////////////////////////////////////////////////////

class TubeStation(port: Int, numberOfTurnstiles: Int) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
     """.stripMargin)
    .withFallback(ConfigFactory.load("advancedclustering/clusterShardingExample.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  // Setting up cluster sharding
  val validatorShardRegionRef: ActorRef = ClusterSharding(system).start(
    typeName = "OysterCardValidator",
    entityProps = Props[OysterCardValidator],
    settings = ClusterShardingSettings(system).withRememberEntities(true),
    extractEntityId = TurnstileSettings.extractEntityId,
    extractShardId = TurnstileSettings.extractShardId
  )

  val turnstiles = (1 to numberOfTurnstiles).map(_ => system.actorOf(Turnstile.props(validatorShardRegionRef)))

  Thread.sleep(10000)

  for (_ <- 1 to 1000) {
    val randomTurnstile = turnstiles(Random.nextInt(numberOfTurnstiles))

    randomTurnstile ! OysterCard(UUID.randomUUID().toString, Random.nextDouble() * 10)
    Thread.sleep(200)
  }
}

object PiccadillyCircus extends TubeStation(2551, 10)

object Westminster extends TubeStation(2561, 5)

object CharingCross extends TubeStation(2571, 15)