package se.kth.benchmarks.akka
import akka.util.ByteStringBuilder

object SerUtils {
  import java.nio.{ByteBuffer, ByteOrder};
  import _root_.akka.util.ByteStringBuilder;

  def stringIntoByteString(bs: ByteStringBuilder, s: String)(implicit order: ByteOrder): Unit = {
    val sBytes = s.getBytes("UTF8");
    bs.putShort(sBytes.size);
    bs.putBytes(sBytes);
  }
  def stringFromByteBuffer(buf: ByteBuffer): String = {
    val pathLength = buf.getShort;
    val pathBytes = new Array[Byte](pathLength);
    buf.get(pathBytes)
    new String(pathBytes, "UTF8")
  }
}
