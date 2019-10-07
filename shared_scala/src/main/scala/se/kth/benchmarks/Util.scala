package se.kth.benchmarks

import scala.util.Try

case class GrpcAddress(addr: String, port: Int)

object Util {
  def argToAddr(s: String): Try[GrpcAddress] = {
    Try {
      val sarr = s.split(":");
      assert(sarr.length == 2);
      val port = sarr(1).toInt;
      GrpcAddress(sarr(0), port)
    }
  }

  def forceShutdown(): Unit = {
    val thread = new Thread {
      override def run {
        Thread.sleep(500);
        Console.err.println("Forcing system shutdown!");
        System.exit(0);
      }
    };
    thread.start();
  }

  def shutdownLater(shutdownCallback: () => Unit): Unit = {
    val thread = new Thread {
      override def run {
        Thread.sleep(500);
        shutdownCallback();
      }
    };
    thread.start();
  }
}
