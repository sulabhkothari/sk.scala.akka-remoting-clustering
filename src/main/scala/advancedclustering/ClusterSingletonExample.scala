package advancedclustering

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory

import scala.util.Random

/*
  A small payment system - CENTRALIZED
   - maintaining a well defined transaction ordering
   - interacting with legacy systems
   - etc
 */

case class Order(items: List[String], total: Double)

case class Transaction(orderId: Int, txnId: String, amount: Double)

class PaymentSystem extends Actor with ActorLogging {
  override def receive: Receive = {
    case t: Transaction => // add complex transaction processing logic
      log.info(s"Validating transaction: $t")
  }
}

class PaymentSystemNode(port: Int, shouldStartSingleton: Boolean = true) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
     """.stripMargin)
    .withFallback(ConfigFactory.load("advancedclustering/clusterSingletonExample.conf"))

  val system = ActorSystem("RTJVMCluster", config)
  if (shouldStartSingleton)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props[PaymentSystem],
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)
      ),
      "paymentSystem"
    )
}

//Logged message after starting below apps - Singleton manager starting singleton actor. This is logged in Node1 because
// singleton is deployed in the oldest node

/*
  On terminating node1, nodes2 becomes the oldest so it takes the handover of the singleton
   - Singleton terminated, hand-over done [akka://RTJVMCluster@localhost:2551 -> Some(akka://RTJVMCluster@localhost:2552)]
   - Node2 logs: ClusterSingletonManager state change [Younger -> BecomingOldest],
   - contd..: ClusterSingletonManager state change [BecomingOldest -> Oldest]
 */

object Node1 extends PaymentSystemNode(2551)

object Node2 extends PaymentSystemNode(2552)

object Node3 extends PaymentSystemNode(2553, false)

class OnlineShopCheckout(paymentSystem: ActorRef) extends Actor with ActorLogging {
  var orderId = 0

  override def receive: Receive = {
    case Order(_, totalAmount) =>
      log.info(s"Received order: $orderId for amount: $totalAmount, sending transaction to validate")
      val newTransaction = Transaction(orderId, UUID.randomUUID().toString, totalAmount)
      paymentSystem ! newTransaction
      orderId += 1
  }
}

object OnlineShopCheckout {
  def props(paymentSystem: ActorRef) = Props(new OnlineShopCheckout(paymentSystem))
}

object PaymentSystemClient extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = 0
     """.stripMargin)
    .withFallback(ConfigFactory.load("advancedclustering/clusterSingletonExample.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  // Cluster singleton can only be communicated with, via Proxy
  val proxy = system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/paymentSystem",
      settings = ClusterSingletonProxySettings(system)
    ),
    "paymentSystemProxy"
  )

  val onlineShopCheckout = system.actorOf(OnlineShopCheckout.props(proxy))

  import system.dispatcher
  import scala.concurrent.duration._

  system.scheduler.schedule(5 seconds, 1 second, () => {
    val randomOrder = Order(List(), Random.nextDouble() * 100)
    onlineShopCheckout ! randomOrder
  })
}