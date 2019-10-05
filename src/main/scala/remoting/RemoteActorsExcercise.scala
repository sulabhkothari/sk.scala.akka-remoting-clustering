package remoting

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSelection, ActorSystem, AddressFromURIString, Deploy, Identify, PoisonPill, Props}
import akka.remote.RemoteScope
import akka.remote.routing.RemoteRouterConfig
import akka.routing.{FromConfig, RoundRobinPool}
import com.typesafe.config.ConfigFactory

object WordCountDomain {

  case class Initialize(nWorkers: Int)

  case class WordCountTask(text: String)

  case class WordCountResult(count: Int)

  case object EndWordCount

}

class WordCountWorker extends Actor with ActorLogging {

  import WordCountDomain._

  override def preStart(): Unit = {
    log.info(s"Starting")
  }

  override def receive: Receive = {
    case WordCountTask(text) =>
      log.info(s"I'm processing: $text")
      sender() ! WordCountResult(text.split(" ").length)
  }
}

class WordCountMaster extends Actor with ActorLogging {

  import WordCountDomain._

  /*override*/ def receive1: Receive = {
    case Initialize(nWorkers) =>
      /*
       TODO 1: identify the workers in the remote JVM
       - create actor selection for every worker
       - send identify messages to the actor selection
       - get into an intialization state, while you are receiving ActorIdentities
       */

      log.info(s"Init Master with $nWorkers workers")
      (1 to nWorkers).map(i => context.actorSelection(s"akka://WorkerSystem@localhost:2552/user/wordCountWorkers$i"))
        .zipWithIndex
        .foreach {
          case (actorSelection, index) =>
            actorSelection ! Identify(index)
        }
      context.become(getActorIdentity(List(), nWorkers))
  }

  def getActorIdentity(workers: List[ActorRef], remainingWorkers: Int): Receive = {
    case ActorIdentity(_, Some(actorRef)) =>
      log.info(s"Worker identified: $actorRef")
      val workerList = actorRef :: workers
      if (remainingWorkers == 1) context.become(online(workerList, 0, 0))
      else context.become(getActorIdentity(workerList, remainingWorkers - 1))
  }

  def online(workers: List[ActorRef], remainingTasks: Int, totalCount: Int): Receive = {
    case text: String =>
      log.info(s"(RemainingTasks: $remainingTasks) Received a task for '$text'")
      val sentences = text.split("\\. ")
      Iterator.continually(workers).flatten.zip(sentences.iterator).foreach {
        case (worker, sentence) => worker ! WordCountTask(sentence)
      }
      context.become(online(workers, remainingTasks + sentences.length, totalCount))
    case WordCountResult(count) =>
      //log.info(s"Received a result from worker: $count, remainingTask: $remainingTasks, totalCount: $totalCount")
      if (remainingTasks == 1) {
        log.info(s"Total result: ${totalCount + count}")
        workers.foreach(_ ! PoisonPill)
      }
      else {
        context.become(online(workers, remainingTasks - 1, totalCount + count))
      }
  }

  /*override*/ def receive2: Receive = {
    case Initialize(nWorkers) =>
      // TODO 2: deploy the workers REMOTELY on the workers app
      log.info(s"Init Master with $nWorkers workers")
      val workers = (1 to nWorkers).map(id => context.actorOf(Props[WordCountWorker], s"wordCountWorker$id"))
      context.become(online(workers.toList, 0, 0))
  }

  override def receive: Receive = {
    case Initialize(nWorkers) =>
      val poolRouter: ActorRef = context.actorOf(
        Props[WordCountWorker].withRouter(RemoteRouterConfig(RoundRobinPool(nWorkers),
          List(AddressFromURIString("akka://WorkerSystem@localhost:2552")))),
        "workerRouter")
      context.become(onlineWithRouter(poolRouter, 0, 0))

  }

  def onlineWithRouter(actorRef: ActorRef, remainingTasks: Int, totalCount: Int): Receive = {
    case text: String =>
      val sentences = text.split("\\. ")
      sentences.foreach { sentence =>
        actorRef ! WordCountTask(sentence)
      }
      context.become(onlineWithRouter(actorRef, remainingTasks + sentences.length, totalCount))

    case WordCountResult(count) =>
      //log.info(s"Received a result from worker: $count, remainingTask: $remainingTasks, totalCount: $totalCount")
      if (remainingTasks == 1) {
        log.info(s"Total result: ${totalCount + count}")
        actorRef ! PoisonPill
      }
      else {
        context.become(onlineWithRouter(actorRef, remainingTasks - 1, totalCount + count))
      }
  }
}

object MasterApp extends App {

  import WordCountDomain._

  val config = ConfigFactory.parseString(
    """
      |akka.remote.artery.canonical.port = 2551
    """.stripMargin
  ).withFallback(ConfigFactory.load("remoting/remoteActorsExcercise.conf"))

  val system = ActorSystem("MasterSystem", config)

  val master = system.actorOf(Props[WordCountMaster], "wordCountMaster")
  master ! Initialize(5)

  Thread.sleep(10000)

  scala.io.Source.fromFile("src/main/resources/txt/lipsum.txt").getLines().foreach(master ! _)
}

object WorkersApp extends App {

  import WordCountDomain._

  val config = ConfigFactory.parseString(
    """
      |akka.remote.artery.canonical.port = 2552
    """.stripMargin
  ).withFallback(ConfigFactory.load("remoting/remoteActorsExcercise.conf"))

  val system = ActorSystem("WorkerSystem", config)

  //(1 to 5).map(i => system.actorOf(Props[WordCountWorker], s"wordCountWorkers$i"))
}