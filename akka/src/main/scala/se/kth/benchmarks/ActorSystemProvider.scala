package se.kth.benchmarks

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Properties._

object ActorSystemProvider {
  private val corePoolSize = getNumWorkers("actors.corePoolSize", 4);
  private val maxPoolSize = getNumWorkers("actors.maxPoolSize", corePoolSize);
  private val priorityMailboxType = getStringProp("actors.mailboxType", "akka.dispatch.SingleConsumerOnlyUnboundedMailbox");

  private val customConfigStr = s"""
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

  private lazy val customConf = ConfigFactory.parseString(customConfigStr);
  lazy val config = ConfigFactory.load(customConf);

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
}
