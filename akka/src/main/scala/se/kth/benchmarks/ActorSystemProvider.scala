package se.kth.benchmarks

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Properties._

object ActorSystemProvider {
  private val corePoolSizeDefault = getNumWorkers("actors.corePoolSize", 4);
  private val maxPoolSizeDefault = getNumWorkers("actors.maxPoolSize", corePoolSizeDefault);
  private val priorityMailboxTypeDefault = getStringProp("actors.mailboxType", "akka.dispatch.SingleConsumerOnlyUnboundedMailbox");

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
}
