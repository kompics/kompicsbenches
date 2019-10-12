package se.kth.benchmarks.helpers
import java.{util => ju}
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale
import scala.reflect.ClassTag

object GraphUtils {

  val WEIGHT_FACTOR = 10.0;
  val WEIGHT_CUTOFF = 5.0;

  def generateGraph(numberOfNodes: Int): Graph[Double] = {
    val random = new ju.Random(numberOfNodes); // always generate the same graph for the same size
    val data = Array.tabulate[Double](numberOfNodes, numberOfNodes) {
      case (i, j) if i == j => 0.0 // self distance better be zero^^
      case _ => {
        val weightRaw = random.nextDouble() * WEIGHT_FACTOR;
        val weight = if (weightRaw > WEIGHT_CUTOFF) {
          Double.PositiveInfinity // no link
        } else {
          weightRaw // weighted link
        };
        weight
      }
    };
    new Graph(data, formatDouble)
  }

  val doubleFormat = {
    val symbols = new DecimalFormatSymbols(Locale.ENGLISH);
    symbols.setInfinity(" +∞  ");
    new DecimalFormat("000.0", symbols);
  }

  private def formatDouble(d: Double): String = doubleFormat.format(d);
}
object Graph {
  def assembleFromBlocks[T: ClassTag](blocks: Array[Array[Block[T]]]): Graph[T] = {
    assert(blocks.size == blocks(0).size, "Block Matrices must be square!");
    val blockSize = blocks(0)(0).blockSize;
    val totalSize = blocks.size * blockSize;
    val data = Array.tabulate[T](totalSize, totalSize) {
      case (i, j) => {
        val blockRow = (i / blockSize);
        val blockCol = (j / blockSize);
        val localRow = i % blockSize;
        val localCol = j % blockSize;
        blocks(blockRow)(blockCol).get(localRow, localCol)
      }
    };
    new Graph(data, blocks(0)(0).formatter)
  }
}
class Graph[T: ClassTag](val data: Array[Array[T]], val formatter: T => String) {
  assert(data.size == data(0).size, "Graph Adjacency Matrices must be square!");

  def numNodes: Int = data.size;

  def getBlock(blockId: Int, blockSize: Int): Block[T] = {
    val numBlocksPerDim = data.size / blockSize;
    val globalStartRow = (blockId / numBlocksPerDim) * blockSize;
    val globalStartCol = (blockId % numBlocksPerDim) * blockSize;

    val block = Array.tabulate[T](blockSize, blockSize) {
      case (i, j) => {
        get(i + globalStartRow, j + globalStartCol)
      }
    };
    new Block(blockId, numBlocksPerDim, globalStartRow, globalStartCol, block, formatter)
  }

  def breakIntoBlocks(blockSize: Int): Array[Array[Block[T]]] = {
    assert(data.size % blockSize == 0, "Only break evenly into blocks!");
    val numBlocksPerDim = data.size / blockSize;
    Array.tabulate[Block[T]](numBlocksPerDim, numBlocksPerDim) {
      case (i, j) => {
        val blockId = (i * numBlocksPerDim) + j;
        this.getBlock(blockId, blockSize)
      }
    }
  }

  def copy(): Graph[T] = {
    val dataCopy = Array.ofDim[T](data.size, data.size);
    for (i <- 0 until data.size) {
      Array.copy(data(i), 0, dataCopy(i), 0, data.size);
    }
    new Graph(dataCopy, formatter)
  }

  def computeFloydWarshall()(implicit ord: Ordering[T], num: Numeric[T]): Unit = {
//     1 let dist be a |V| × |V| array of minimum distances initialized to ∞ (infinity)
// 2 for each edge (u,v)
// 3    dist[u][v] ← w(u,v)  // the weight of the edge (u,v)
// 4 for each vertex v
// 5    dist[v][v] ← 0
// 6 for k from 1 to |V|
// 7    for i from 1 to |V|
// 8       for j from 1 to |V|
// 9          if dist[i][j] > dist[i][k] + dist[k][j]
// 10             dist[i][j] ← dist[i][k] + dist[k][j]
// 11         end if
    for (k <- 0 until data.size; i <- 0 until data.size; j <- 0 until data.size) {
      val newValue = num.plus(this.get(i, k), this.get(k, j));
      if (ord.gt(this.get(i, j), newValue)) {
        this.set(i, j)(newValue);
      }
    }
  }

  def get(i: Int, j: Int): T = {
    data(i)(j)
  }

  def set(i: Int, j: Int)(v: T): Unit = {
    data(i)(j) = v;
  }

  override def toString(): String = {
    s"""
Graph(
${data.map(_.map(formatter).mkString(" ")).mkString("\n")}
)"""
  }

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Graph[T]]) {
      val other = obj.asInstanceOf[Graph[T]];
      if (this.data.size == other.data.size) { // must be square, no need to check inner dims
        for (i <- 0 until this.data.size; j <- 0 until this.data.size) {
          if (this.get(i, j) != other.get(i, j)) {
            return false;
          }
        }
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

}

class Block[T: ClassTag](val blockId: Int,
                         val numBlocksPerDim: Int,
                         val rowOffset: Int,
                         val colOffset: Int,
                         _data: Array[Array[T]],
                         _formatter: T => String)
    extends Graph[T](_data, _formatter) {

  val blockSize = data.size;

  def blockPosition: (Int, Int) = {
    val j = blockId % numBlocksPerDim;
    val i = blockId / numBlocksPerDim;
    (i, j)
  }

  def computeFloydWarshallInner(neighbours: Int => Block[T], k: Int)(
      implicit ord: Ordering[T],
      num: Numeric[T]
  ) = {
    for (i <- 0 until blockSize; j <- 0 until blockSize) {
      val globalI = rowOffset + i;
      val globalJ = colOffset + j;

      val newValue = num.plus(elementAt(globalI, k, neighbours), elementAt(k, globalJ, neighbours));
      val oldValue = this.get(i, j);
      if (ord.gt(oldValue, newValue)) {
        this.set(i, j)(newValue)
      }
    }
  }

  def elementAt(row: Int, col: Int, neighbours: Int => Block[T]): T = {
    val destBlockId = ((row / blockSize) * numBlocksPerDim) + (col / blockSize);
    val localRow = row % blockSize;
    val localCol = col % blockSize;

    if (destBlockId == blockId) {
      this.get(localRow, localCol)
    } else {
      val blockData = neighbours(destBlockId);
      blockData.get(localRow, localCol)
    }
  }

  override def copy(): Block[T] = {
    val dataCopy = Array.ofDim[T](data.size, data.size);
    for (i <- 0 until data.size) {
      Array.copy(data(i), 0, dataCopy(i), 0, data.size);
    }
    new Block(blockId, numBlocksPerDim, rowOffset, colOffset, dataCopy, formatter)
  }
}
