package se.kth.benchmarks.kompicsscala

import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ ComponentDefinition, Init, KompicsEvent, Port, Start, handle }

class BestEffortBroadcast extends Port {
  request[BEBRequest]
  indication[BEBDeliver]
}

case class BEBDeliver(payLoad: KompicsEvent, src: NetAddress) extends KompicsEvent;
case class BEBRequest(nodes: Set[NetAddress], payLoad: KompicsEvent) extends KompicsEvent;

class BEBComp(init: Init[BEBComp]) extends ComponentDefinition {
  val net = requires[Network]
  val beb = provides[BestEffortBroadcast]
  val Init(selfAddr: NetAddress) = init

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
