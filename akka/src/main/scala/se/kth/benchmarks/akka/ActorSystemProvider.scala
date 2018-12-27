package se.kth.benchmarks.akka

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Properties._

object ActorSystemProvider {
  private val corePoolSizeDefault = getNumWorkers("actors.corePoolSize", 4);
  private val maxPoolSizeDefault = getNumWorkers("actors.maxPoolSize", corePoolSizeDefault);
  private val priorityMailboxTypeDefault = getStringProp("actors.mailboxType", "akka.dispatch.SingleConsumerOnlyUnboundedMailbox");

  private var publicIf = "127.0.0.1";
  def setPublicIf(pif: String): Unit = this.synchronized{ publicIf = pif; };
  def getPublicIf(): String = this.synchronized { publicIf };

  private def customConfig(
    corePoolSize:        Int    = corePoolSizeDefault,
    maxPoolSize:         Int    = maxPoolSizeDefault,
    priorityMailboxType: String = priorityMailboxTypeDefault) = s"""
    akka {
      log-dead-letters-during-shutdown = off
      log-dead-letters = off

      actor {
        creation-timeout = 6000s
        default-dispatcher {
          fork-join-executor {
            parallelism-min = ${corePoolSize}
            parallelism-max = ${maxPoolSize}
            parallelism-factor = 1.0
          }
        }
        default-mailbox {
          mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
        }
        prio-dispatcher {
          mailbox-type = "${priorityMailboxType}"
        }
        typed {
          timeout = 10000s
        }
      }
    }""";

  private def remoteConfig(
    corePoolSize:        Int    = corePoolSizeDefault,
    maxPoolSize:         Int    = maxPoolSizeDefault,
    priorityMailboxType: String = priorityMailboxTypeDefault,
    hostname:            String,
    port:                Int) = s"""
    akka {
      log-dead-letters-during-shutdown = off
      log-dead-letters = off

      actor {
        creation-timeout = 6000s
        default-dispatcher {
          fork-join-executor {
            parallelism-min = ${corePoolSize}
            parallelism-max = ${maxPoolSize}
            parallelism-factor = 1.0
          }
        }
        default-mailbox {
          mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
        }
        prio-dispatcher {
          mailbox-type = "${priorityMailboxType}"
        }
        typed {
          timeout = 10000s
        }
        provider = remote
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = "${hostname}"
          port = ${port}
        }
      }
    }""";

  private lazy val defaultConf = ConfigFactory.parseString(customConfig());
  lazy val config = ConfigFactory.load(defaultConf);

  private def getNumWorkers(propertyName: String, minNumThreads: Int): Int = {
    val rt: Runtime = java.lang.Runtime.getRuntime

    getIntegerProp(propertyName) match {
      case Some(i) if i > 0 => i
      case _ => {
        val byCores = rt.availableProcessors() * 2
        if (byCores > minNumThreads) byCores else minNumThreads
      }
    }
  }

  private def getIntegerProp(propName: String): Option[Int] = {
    try {
      propOrNone(propName) map (_.toInt)
    } catch {
      case _: SecurityException | _: NumberFormatException => None
    }
  }

  private def getStringProp(propName: String, defaultVal: String): String = {
    propOrElse(propName, defaultVal)
  }

  def newActorSystem(name: String): ActorSystem = {
    ActorSystem(name, config)
  }
  def newActorSystem(name: String, threads: Int): ActorSystem = {
    val confStr = ConfigFactory.parseString(customConfig(corePoolSize = threads, maxPoolSize = threads));
    val conf = ConfigFactory.load(confStr);
    ActorSystem(name, conf)
  }
  def newRemoteActorSystem(name: String, threads: Int): ActorSystem = {
    val confStr = ConfigFactory.parseString(remoteConfig(corePoolSize = threads, maxPoolSize = threads, hostname = getPublicIf, port = 0));
    val conf = ConfigFactory.load(confStr);
    ActorSystem(name, conf)
  }

  object ExternalAddress extends ExtensionId[ExternalAddressExt] with ExtensionIdProvider {
    override def lookup() = ExternalAddress

    override def createExtension(system: ExtendedActorSystem): ExternalAddressExt =
      new ExternalAddressExt(system)

    override def get(system: ActorSystem): ExternalAddressExt = super.get(system)
  }

  class ExternalAddressExt(system: ExtendedActorSystem) extends Extension {
    def addressForAkka: Address = system.provider.getDefaultAddress
  }

  def actorPathForRef(ref: ActorRef, system: ActorSystem): String = {
    val addr = ExternalAddress(system).addressForAkka;
    s"akka.tcp://${addr}/${ref.path}"
  }
}
