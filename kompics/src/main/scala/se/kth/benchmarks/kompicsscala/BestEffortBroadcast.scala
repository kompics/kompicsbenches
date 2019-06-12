package se.kth.benchmarks.kompicsscala

import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ ComponentDefinition, KompicsEvent, Port, Start, handle }

class BestEffortBroadcast extends Port {
  request[BEBRequest]
  indication[BEBDeliver]
}

case class BEBDeliver(payLoad: KompicsEvent, src: NetAddress) extends KompicsEvent;
case class BEBRequest(nodes: Set[NetAddress], payLoad: KompicsEvent) extends KompicsEvent;

class BEBComp(selfAddr: NetAddress) extends ComponentDefinition {
  val net = requires[Network]
  val beb = provides[BestEffortBroadcast]

  ctrl uponEvent {
    case _: Start => handle {
      assert(selfAddr != null)
    }
  }

  net uponEvent {
    case NetMessage(src, payLoad) => handle {
      trigger(BEBDeliver(payLoad, src.getSource()) -> beb)
    }
  }

  beb uponEvent {
    case BEBRequest(nodes, payLoad) => handle {
      for (addr: NetAddress <- nodes) {
        trigger(NetMessage.viaTCP(selfAddr, addr)(payLoad) -> net)
      }
    }
  }

}
