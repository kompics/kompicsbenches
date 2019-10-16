package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class DoubleGraph {
    protected final double[][] data;

    public final int numNodes;

    public DoubleGraph(double[][] data) {
        assert data.length == data[0].length : "Graph Adjacency Matrices must be square!";

        this.data = data;
        this.numNodes = data.length;
    }

    public DoubleGraph copy() {
        double[][] dataCopy = new double[data.length][data.length];
        for (int i = 0; i < data.length; i++) {
            System.arraycopy(data[i], 0, dataCopy[i], 0, data.length);
        }
        return new DoubleGraph(dataCopy);
    }

    public DoubleBlock getBlock(int blockId, int blockSize) {
        int numBlocksPerDim = data.length / blockSize;
        int globalStartRow = (blockId / numBlocksPerDim) * blockSize;
        int globalStartCol = (blockId % numBlocksPerDim) * blockSize;

        double[][] blockData = new double[blockSize][blockSize];
        for (int i = 0; i < blockSize; i++) {
            for (int j = 0; j < blockSize; j++) {
                blockData[i][j] = this.get(i + globalStartRow, j + globalStartCol);
            }
        }
        return new DoubleBlock(blockId, numBlocksPerDim, globalStartRow, globalStartCol, blockData);
    }

    public DoubleBlock[][] breakIntoBlocks(int blockSize) {
        assert data.length % blockSize == 0 : "Only break evenly into blocks!";

        int numBlocksPerDim = data.length / blockSize;
        DoubleBlock[][] blocks = new DoubleBlock[numBlocksPerDim][numBlocksPerDim];
        for (int i = 0; i < numBlocksPerDim; i++) {
            for (int j = 0; j < numBlocksPerDim; j++) {
                int blockId = (i * numBlocksPerDim) + j;
                blocks[i][j] = this.getBlock(blockId, blockSize);
            }
        }
        return blocks;
    }

    public static DoubleGraph assembleFromBlocks(DoubleBlock[][] blocks) {
        assert blocks.length == blocks[0].length : "Block Matrices must be square!";

        int blockSize = blocks[0][0].blockSize;
        int totalSize = blockSize * blocks.length;
        double[][] data = new double[totalSize][totalSize];
        for (int i = 0; i < totalSize; i++) {
            for (int j = 0; j < totalSize; j++) {
                int blockRow = i / blockSize;
                int blockCol = j / blockSize;
                int localRow = i % blockSize;
                int localCol = j % blockSize;
                data[i][j] = blocks[blockRow][blockCol].get(localRow, localCol);
            }
        }
        return new DoubleGraph(data);
    }

    public void computeFloydWarshall() {
        for (int k = 0; k < data.length; k++) {
            for (int i = 0; i < data.length; i++) {
                for (int j = 0; j < data.length; j++) {
                    double newValue = this.get(i, k) + this.get(k, j);
                    if (this.get(i, j) > newValue) {
                        this.set(i, j, newValue);
                    }
                }
            }
        }
    }

    public double get(int i, int j) {
        return data[i][j];
    }

    public void set(int i, int j, double value) {
        data[i][j] = value;
    }

    public static final DecimalFormat FORMATTER;
    static {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.ENGLISH);
        symbols.setInfinity(" +∞  ");
        FORMATTER = new DecimalFormat("000.0", symbols);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Graph(\n");
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data.length; j++) {
                sb.append(FORMATTER.format(data[i][j]));
                sb.append(' ');
            }
            sb.append('\n');
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleGraph) {
            DoubleGraph other = (DoubleGraph) obj;
            if (this.data.length == other.data.length) { // must be square, no need to check inner dims
                for (int i = 0; i < data.length; i++) {
                    for (int j = 0; j < data.length; j++) {
                        if (this.get(i, j) != other.get(i, j)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }
}
