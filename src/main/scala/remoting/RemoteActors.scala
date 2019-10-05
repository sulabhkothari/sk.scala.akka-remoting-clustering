package remoting

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSelection, ActorSystem, Identify, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

object RemoteActors extends App {
  val localSystem = ActorSystem("local-system", ConfigFactory.load("remoting/remoteActors.conf"))
  //val remoteSystem = ActorSystem("remote-system", ConfigFactory.load("remoting/remoteActors.conf").getConfig("remoteSystem"))
  val localSimpleActor = localSystem.actorOf(Props[SimpleActor], "simple-local-actor")
  //val remoteSimpleActor = remoteSystem.actorOf(Props[SimpleActor], "remote-local-actor")

  localSimpleActor ! "hello, local actor!"
  //remoteSimpleActor ! "hello, remote actor!"

  // send a message to the remote simple actor

  // METHOD 1: Actor selection
  val remoteActorSelection: ActorSelection = localSystem.actorSelection("akka://remote-system@localhost:2552/user/simple-remote-actor")
  remoteActorSelection ! "hello from the \"local\" JVM"

  // METHOD 2: Resolve the actor selection to an actor ref
  import localSystem.dispatcher
  import scala.concurrent.duration._

  implicit val timeout = Timeout(3 seconds)
  val remoteActorRefFuture: Future[ActorRef] = remoteActorSelection.resolveOne()
  remoteActorRefFuture.onComplete {
    case Success(actorRef) => actorRef ! "I've resolved you in a future"
    case Failure(exception) => println(s"I failed to resolve the remote actor because $exception")
  }

  // METHOD 3: Actor identification via messages
  /*
    - actor resolver will ask for an actor selection from the local actor system
    - actor resolver will send an Identify(42) to the actor selection
    - the remote actor will AUTOMATICALLY respond with ActorIdentity(42, actorRef)
    - the actor resolver is free to use the remote actorRef
   */
  class ActorResolver extends Actor with ActorLogging{
    override def preStart(): Unit = {
      val selection = context.actorSelection("akka://remote-system@localhost:2552/user/simple-remote-actor")
      selection ! Identify(42)
    }
    override def receive: Receive = {
      case ActorIdentity(42, Some(actorRef)) =>
        actorRef ! "Thank you for identifying yourself"
    }
  }
  localSystem.actorOf(Props[ActorResolver], "localActorResolver")
}

object RemoteActors_Remote extends App {
  val remoteSystem = ActorSystem("remote-system", ConfigFactory.load("remoting/remoteActors.conf").getConfig("remoteSystem"))
  val remoteSimpleActor = remoteSystem.actorOf(Props[SimpleActor], "simple-remote-actor")
  remoteSimpleActor ! "hello, remote actor!"
}