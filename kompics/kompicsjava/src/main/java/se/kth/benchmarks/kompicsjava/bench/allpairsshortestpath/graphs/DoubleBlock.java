package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs;

import java.util.Map;

public class DoubleBlock extends DoubleGraph {

    public final int blockId;
    public final int numBlocksPerDim;
    public final int blockSize;

    final int rowOffset;
    final int colOffset;

    public DoubleBlock(int blockId, int numBlocksPerDim, int rowOffset, int colOffset, double[][] data) {
        super(data);

        this.blockId = blockId;
        this.numBlocksPerDim = numBlocksPerDim;
        this.rowOffset = rowOffset;
        this.colOffset = colOffset;
        this.blockSize = data.length;
    }

    public int blockPositionI() {
        return blockId / numBlocksPerDim;
    }

    public int blockPositionJ() {
        return blockId % numBlocksPerDim;
    }

    public void computeFloydWarshallInner(Map<Integer, DoubleBlock> neighbours, int k) {
        for (int i = 0; i < blockSize; i++) {
            for (int j = 0; j < blockSize; j++) {
                int globalI = rowOffset + i;
                int globalJ = colOffset + j;

                double newValue = this.elementAt(globalI, k, neighbours) + this.elementAt(k, globalJ, neighbours);
                if (this.get(i, j) > newValue) {
                    this.set(i, j, newValue);
                }
            }
        }
    }

    public double elementAt(int row, int col, Map<Integer, DoubleBlock> neighbours) {
        int destBlockId = ((row / blockSize) * numBlocksPerDim) + (col / blockSize);
        int localRow = row % blockSize;
        int localCol = col % blockSize;

        if (destBlockId == this.blockId) {
            return this.get(localRow, localCol);
        } else {
            DoubleBlock blockData = neighbours.get(destBlockId);
            return blockData.get(localRow, localCol);
        }
    }

    public DoubleBlock copy() {
        double[][] dataCopy = new double[data.length][data.length];
        for (int i = 0; i < data.length; i++) {
            System.arraycopy(data[i], 0, dataCopy[i], 0, data.length);
        }
        return new DoubleBlock(blockId, numBlocksPerDim, rowOffset, colOffset, dataCopy);
    }

}
