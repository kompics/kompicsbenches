package se.kth.benchmarks.akka

import akka.actor._
import akka.actor.typed.Behavior
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.util.Properties._
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

final case class SerializerName(name: String);
final case class ClassRef(clazz: Class[_]) {
  def fullName: String = clazz.getName; // canonical name gives wrong $
}
object ClassRef {
  def from[C](implicit objTag: ClassTag[C]): ClassRef = {
    ClassRef(objTag.runtimeClass)
  }
}
final class SerializerBindings(val serializers: Map[SerializerName, ClassRef],
                               val bindings: Map[SerializerName, List[ClassRef]]) {

  def addSerializer[Serializer](name: String)(implicit serTag: ClassTag[Serializer]): SerializerBindings = {
    val serClazz = ClassRef(serTag.runtimeClass);
    val newSerializers = serializers + (SerializerName(name) -> serClazz);
    new SerializerBindings(newSerializers, bindings)
  }

  def addBinding[Serializee](name: String)(implicit objTag: ClassTag[Serializee]): SerializerBindings = {
    val objClazz = ClassRef(objTag.runtimeClass);
    val serName = SerializerName(name);
    val newEntry = bindings.get(serName) match {
      case Some(l) => objClazz :: l
      case None    => List(objClazz)
    };
    val newBindings = bindings + (serName -> newEntry);
    new SerializerBindings(serializers, newBindings)
  }

  //  private[akka] addDefaults(): SerializerBindings = {
  //
  //  }
}
object SerializerBindings {
  def empty(): SerializerBindings = new SerializerBindings(Map.empty, Map.empty);
  def withBinding[Serializer, Serializee](name: String)(implicit serTag: ClassTag[Serializer],
                                                        objTag: ClassTag[Serializee]): SerializerBindings = {
    val serClazz = ClassRef(serTag.runtimeClass);
    val objClazz = ClassRef(objTag.runtimeClass);
    val serName = SerializerName(name);
    new SerializerBindings(Map(serName -> serClazz), Map(serName -> List(objClazz)))
  }
}

object ActorSystemProvider extends StrictLogging {
  private val corePoolSizeDefault = getNumWorkers("actors.corePoolSize", 4);
  private val maxPoolSizeDefault = getNumWorkers("actors.maxPoolSize", corePoolSizeDefault);
  private val priorityMailboxTypeDefault =
    getStringProp("actors.mailboxType", "akka.dispatch.SingleConsumerOnlyUnboundedMailbox");

  private var publicIf = "127.0.0.1";
  def setPublicIf(pif: String): Unit = this.synchronized { publicIf = pif; };
  def getPublicIf(): String = this.synchronized { publicIf };

  private def customConfig(corePoolSize: Int = corePoolSizeDefault,
                           maxPoolSize: Int = maxPoolSizeDefault,
                           priorityMailboxType: String = priorityMailboxTypeDefault) = s"""
    akka {
      log-dead-letters-during-shutdown = off
      log-dead-letters = off
      loggers = ["akka.event.slf4j.Slf4jLogger"]
      loglevel = "WARNING"
      logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"

      actor {
        creation-timeout = 6000s
        default-dispatcher {
          fork-join-executor {
            parallelism-min = ${corePoolSize}
            parallelism-max = ${maxPoolSize}
            parallelism-factor = 1.0
          }
          throughput = 50
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

  private def remoteConfig(corePoolSize: Int = corePoolSizeDefault,
                           maxPoolSize: Int = maxPoolSizeDefault,
                           priorityMailboxType: String = priorityMailboxTypeDefault,
                           hostname: String,
                           port: Int,
                           customSerializers: SerializerBindings) = {
    val serializers = customSerializers.serializers
      .map(e => s"""          ${e._1.name} = "${e._2.fullName}" """)
      .mkString("serializers {\n", "\n", "\n        }");
    val bindings = customSerializers.bindings.toList
      .flatMap(e => e._2.map(c => (e._1, c)))
      .map(e => s"""          "${e._2.fullName}" = ${e._1.name}""")
      .mkString("serialization-bindings {\n", "\n", "\n        }");

    val conf = s"""
    akka {
      log-dead-letters-during-shutdown = off
      log-dead-letters = off
      loggers = ["akka.event.slf4j.Slf4jLogger"]
      loglevel = "WARNING"
      logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
      remote {
        artery {
          advanced {
            outbound-message-queue-size = 80000
          }
        }
      }
      actor {
        creation-timeout = 6000s
        default-dispatcher {
          fork-join-executor {
            parallelism-min = ${corePoolSize}
            parallelism-max = ${maxPoolSize}
            parallelism-factor = 1.0
          }
          throughput = 50
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
        allow-java-serialization=off
        remote {
          artery {
            transport = tcp
            canonical.hostname = "${hostname}"
            canonical.port = ${port}
          }
        }
        ${serializers}
        ${bindings}
      }
    }""";
    logger.trace(conf);
    conf
  };
      /*
      remote.artery.enabled = false
      remote.classic {
        enabled-transports = ["akka.remote.classic.netty.tcp"]
        netty.tcp {
          hostname = "${hostname}"
          port = ${port}
        }
      }
      */

  private lazy val defaultConf = ConfigFactory.parseString(customConfig());
  lazy val config = ConfigFactory.load(defaultConf);

  private def getNumWorkers(propertyName: String, minNumThreads: Int): Int = {
    val rt: Runtime = java.lang.Runtime.getRuntime

    getIntegerProp(propertyName) match {
      case Some(i) if i > 0 => i
      case _ => {
        val byCores = rt.availableProcessors() // already includes HT
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
  def newRemoteActorSystem(name: String, threads: Int, serialization: SerializerBindings): ActorSystem = {
    val confStr = ConfigFactory.parseString(
      remoteConfig(corePoolSize = threads,
                   maxPoolSize = threads,
                   hostname = getPublicIf,
                   port = 0,
                   customSerializers = serialization)
    );
    val conf = ConfigFactory.load(confStr);
    ActorSystem(name, conf)
  }

  def newTypedActorSystem[T](behavior: typed.Behavior[T], name: String): typed.ActorSystem[T] = {
    typed.ActorSystem(behavior, name, config)
  }

  def newTypedActorSystem[T](behavior: typed.Behavior[T], name: String, threads: Int): typed.ActorSystem[T] = {
    val confStr = ConfigFactory.parseString(customConfig(corePoolSize = threads, maxPoolSize = threads));
    val conf = ConfigFactory.load(confStr);
    typed.ActorSystem(behavior, name, conf)
  }

  def newRemoteTypedActorSystem[T](behavior: typed.Behavior[T],
                                   name: String,
                                   threads: Int,
                                   serialization: SerializerBindings): typed.ActorSystem[T] = {
    val confStr = ConfigFactory.parseString(
      remoteConfig(corePoolSize = threads,
                   maxPoolSize = threads,
                   hostname = getPublicIf,
                   port = 0,
                   customSerializers = serialization)
    );
    val conf = ConfigFactory.load(confStr);
    typed.ActorSystem[T](behavior, name, conf)
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
    ref.path.toStringWithAddress(addr)
  }
}
