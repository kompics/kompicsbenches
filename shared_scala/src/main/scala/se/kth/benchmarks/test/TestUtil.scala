package se.kth.benchmarks.test;

import io.grpc.ManagedChannel;
import com.typesafe.scalalogging.StrictLogging

object TestUtil extends StrictLogging {
  def shutdownChannel(channel: ManagedChannel): Unit = {
    try {
      channel.shutdown();
      if (!channel.awaitTermination(500, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        channel.shutdownNow();
        if (!channel.awaitTermination(500, java.util.concurrent.TimeUnit.MILLISECONDS)) {
          logger.error("gRPC channel never shut down. Giving up...");
        } else {
          logger.info("gRPC channel terminated!");
        }
      } else {
        logger.info("gRPC channel terminated!");
      }
    } catch {
      case e: Throwable => logger.error("gRPC channel shutdown failed!", e);
    }
  }
}
