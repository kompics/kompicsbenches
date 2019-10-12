package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs;

import java.util.Random;

public abstract class GraphUtils {
    public static final double WEIGHT_FACTOR = se.kth.benchmarks.helpers.GraphUtils$.MODULE$.WEIGHT_FACTOR();
    public static final double WEIGHT_CUTOFF = se.kth.benchmarks.helpers.GraphUtils$.MODULE$.WEIGHT_CUTOFF();

    public static DoubleGraph generateGraph(int numberOfNodes) {
        Random rand = new Random(numberOfNodes); // always generate the same graph for the same size
        double[][] data = new double[numberOfNodes][numberOfNodes];
        for (int i = 0; i < numberOfNodes; i++) {
            for (int j = 0; j < numberOfNodes; j++) {
                if (i == j) {
                    data[i][j] = 0.0; // self distance must be zero
                } else {
                    double weight = rand.nextDouble() * WEIGHT_FACTOR;
                    if (weight > WEIGHT_CUTOFF) {
                        data[i][j] = Double.POSITIVE_INFINITY;
                    } else {
                        data[i][j] = weight;
                    }
                }
            }
        }
        return new DoubleGraph(data);
    }

}
